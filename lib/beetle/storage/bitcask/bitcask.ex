defmodule Beetle.Storage.Bitcask do
  @moduledoc """
  Bitcask is a log-structured key-value store designed to handle
  production-grade traffic. 

  It uses a lot of principles from log-structured file systems and draws
  inspiration from a number of designs that involve log file merging. It
  essentially is just a directory of append-only files with a fixed structure
  and an in-memory index holding the keys mapped to a bunch of information
  necessary for point lookups.
  """
  require Logger
  alias Beetle.Storage.Bitcask.{Datafile, Keydir}

  @typedoc """
  Represents bitcask database struct.

  It contains the state required to manage a single bitcask instance. The
  fields are:
  - `:path`: path to the database directory where all datafiles are stored.
    Must be valid directory path that the process has permission to read and
    write to.
  - `:keydir`: in-memory key directory mapping keys to their locations in the
    datafiles. Maintains the latest value position for each key for faster
    lookups.
  - `:active_datafile_id`: ID of the currently active datafile where new writes
    are appended. When a file reaches its size limit, a new active file is
    created with an incremented ID.
  - `:file_handles`: a map of file IDs to their corresponding file handles.
    Maintains open file descriptors for all datafiles.
  """
  @type t :: %__MODULE__{
          path: Path.t(),
          keydir: Keydir.t(),
          file_handles: Datafile.map_t(),
          active_datafile_id: Datafile.file_id_t()
        }

  @type put_opts_t :: [expiration: non_neg_integer()]

  defstruct(path: "", keydir: nil, active_datafile_id: 0, file_handles: nil)

  @doc "Creates a new Bitcask instance at the specified path"
  @spec new(Path.t()) :: {:ok, t()} | {:error, term()}
  def new(path) do
    with :ok <- File.mkdir_p(path),
         {:ok, datafiles} <- Datafile.open(path),
         {:ok, keydir} <- Keydir.new(path, datafiles),
         active_datafile_id <- map_size(datafiles) + 1,
         {:ok, active_datafile} <- path |> Datafile.path(active_datafile_id) |> Datafile.new() do
      {:ok,
       %__MODULE__{
         path: path,
         keydir: keydir,
         active_datafile_id: active_datafile_id,
         file_handles: Map.put(datafiles, active_datafile_id, active_datafile)
       }}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Close the Bitcask instance"
  @spec close(t()) :: :ok | {:error, String.t()}
  def close(store) do
    with :ok <- Keydir.persist(store.keydir, store.path),
         :ok <-
           Enum.reduce_while(store.file_handles, :ok, fn {_, file_handle}, _acc ->
             case Datafile.close(file_handle) do
               {:error, reason} -> {:halt, {:error, reason}}
               :ok -> {:cont, :ok}
             end
           end) do
      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Retrieves the entry stored against a key from the Bitcask database."
  @spec get(t(), Datafile.Entry.key_t()) :: {:ok, Datafile.Entry.t() | nil} | {:error, String.t()}
  def get(store, key) do
    case Keydir.get(store.keydir, key) do
      nil ->
        {:ok, nil}

      %{file_id: file_id, value_pos: pos, value_size: size} ->
        datafile = store.file_handles[file_id]
        Datafile.get(datafile, pos, size)
    end
  end

  @doc """
  Writes a key-value pair in the database with expiration. 

  A value of 0 denotes no expiration. Otherwise, expiration is a UNIX timestamp
  in milliseconds.
  """
  @spec put(t(), Datafile.Entry.key_t(), Datafile.Entry.value_t(), put_opts_t()) ::
          {:ok, t()} | {:error, String.t()}
  def put(store, key, value, opts \\ [expiration: 0]) do
    file_id = store.active_datafile_id
    active_datafile = store.file_handles[file_id]

    case Datafile.write(active_datafile, key, value, opts) do
      {:ok, updated_datafile} ->
        file_handles = Map.put(store.file_handles, file_id, updated_datafile)

        keydir_value = %{
          file_id: file_id,
          value_pos: active_datafile.offset,
          value_size: updated_datafile.offset - active_datafile.offset
        }

        updated_keydir = Keydir.put(store.keydir, key, keydir_value)

        {:ok, %__MODULE__{store | file_handles: file_handles, keydir: updated_keydir}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Delete key(s) from the store.

  This operation doesn't immediately removes the key but overwrites it with a
  tombstone value. The deleted keys are cleaned up during merging operation. It
  returns a non-negative integer specifying the count of keys deleted.
  """
  @spec delete(t(), [Datafile.Entry.key_t()]) :: {t(), non_neg_integer()}
  def delete(store, keys) do
    keys
    |> Enum.reduce({store, 0}, fn key, {store_acc, deleted_keys} ->
      tombstone_value = Datafile.Entry.tombstone_value()

      case put(store, key, tombstone_value) do
        {:ok, updated_store} ->
          {updated_store, deleted_keys + 1}

        {:error, reason} ->
          Logger.error("#{__MODULE__} failed to delete key: #{key}, error: #{inspect(reason)}")
          {store_acc, deleted_keys}
      end
    end)
  end

  @doc """
  Performs compaction on the store.

  The merge operation in Bitcask is a compaction process that reclaims disk
  space by removing stale or redundant data entries. During normal operations,
  Bitcask appends all writes to the active datfile, including updates and
  deletions. This append-only design means that the older version of values and
  deleted entries still occupy disk space until a merge is performed.
  """
  @spec compaction(t()) :: {:ok, t()} | {:error, term()}
  def compaction(store) when map_size(store.file_handles) < 2, do: {:ok, store}

  def compaction(store) do
    merge_dir = store.path |> Path.join("merge") |> to_charlist()
    merge_datafile_path = Datafile.path(merge_dir, 0)

    with :ok <- :file.make_dir(merge_dir),
         {:ok, merge_datafile} <- Datafile.new(merge_datafile_path),
         {:ok, merge_keydir} <- populate_valid_entries(store.file_handles, merge_datafile),
         :ok <- remove_stale_datafiles(store.path),
         :ok <- :file.rename(merge_dir, Datafile.path(store.path, 0)),
         :ok <- :file.del_dir_r(merge_dir),
         :ok <- Keydir.persist(merge_keydir, store.path) do
      {:ok,
       %__MODULE__{
         store
         | file_handles: %{0 => merge_datafile},
           active_datafile_id: 0,
           keydir: merge_keydir
       }}
    else
      {:error, reason} ->
        :file.del_dir_r(merge_dir)
        {:error, reason}
    end
  end

  @doc """
  Log rotation, in Bitcask, is the process of creating new active datafile when
  certain conditions are met. 

  Beetle makes use of file size to kickoff log rotation i.e. if the active
  datafile size exceeds certain threshold, new active datafile will be created.
  Unlike traditional log rotation that might delete old files, Bitcask's
  rotation creates new files while preserving old ones, mainting an append only
  storage model. The older files are usually cleaned during compaction process.
  """
  @spec log_rotation(t()) :: {:ok, t()} | {:error, term()}
  def log_rotation(store) do
    new_file_id = store.active_datafile_id + 1
    path = Datafile.path(store.path, new_file_id)

    case Datafile.new(path) do
      {:ok, new_datafile} ->
        file_handles = Map.put(store.file_handles, new_file_id, new_datafile)
        {:ok, %__MODULE__{store | active_datafile_id: new_file_id, file_handles: file_handles}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc "Force any pending writes to sync to disk."
  @spec sync(t()) :: :ok
  def sync(store) do
    datafile = store.file_handles[store.active_datafile_id]
    Datafile.sync(datafile)
  end

  ########## Private

  @spec populate_valid_entries(Datafile.map_t(), Datafile.t()) ::
          {:ok, Keydir.t()} | {:error, term()}
  defp populate_valid_entries(datafiles, merge_datafile) do
    datafiles
    |> Task.async_stream(fn {_, datafile} -> Datafile.scan_valid_entries(datafile) end,
      ordered: false,
      timeout: :timer.seconds(10),
      max_concurrency: System.schedulers_online() * 2
    )
    |> Enum.reduce_while({merge_datafile, %{}}, fn
      {:ok, stream}, {datafile, keydir} ->
        case process_entries_batch(stream, datafile, keydir) do
          {:error, reason} -> {:halt, {:error, reason}}
          {updated_datafile, updated_keydir} -> {:cont, {updated_datafile, updated_keydir}}
        end

      {:exit, reason}, _ ->
        {:halt, {:error, {:scan_failed, reason}}}
    end)
    |> case do
      {:error, reason} -> {:error, reason}
      {_, keydir} -> {:ok, keydir}
    end
  end

  @spec process_entries_batch(Enumerable.t(), Datafile.t(), Keydir.t()) ::
          {Datafile.t(), Keydir.t()} | {:error, String.t()}
  defp process_entries_batch(entries, datafile, keydir) do
    Enum.reduce_while(entries, {datafile, keydir}, fn entry, {datafile_acc, keydir_acc} ->
      case Datafile.write(datafile_acc, entry.key, entry.value, expiration: entry.expiration) do
        {:ok, updated_datafile} ->
          keydir_value = %{
            file_id: 0,
            value_size: entry.size,
            value_pos: updated_datafile.offset - datafile_acc.offset
          }

          updated_keydir = Keydir.put(keydir_acc, entry.key, keydir_value)

          {:cont, {updated_datafile, updated_keydir}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
  end

  @spec remove_stale_datafiles(Path.t()) :: :ok
  defp remove_stale_datafiles(path) do
    path
    |> Path.join("beetle_*.db")
    |> Path.wildcard()
    |> Enum.each(&File.rm/1)
  end
end
