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
  import Beetle.Utils

  alias Beetle.Storage.Bitcask.{
    Keydir,
    Datafile
  }

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
  - `:active_file`: ID of the currently active datafile where new writes are
    appended. When a file reaches its size limit, a new active file is created
    with an incremented ID.
  - `:file_handles`: a map of file IDs to their corresponding file handles.
    Maintains open file descriptors for all datafiles.
  """
  @type t :: %__MODULE__{
          path: Path.t(),
          keydir: Keydir.t(),
          active_file: Datafile.file_id_t(),
          file_handles: Datafile.map_t()
        }

  defstruct(
    path: "",
    keydir: nil,
    active_file: 0,
    file_handles: nil
  )

  @doc "Creates a new Bitcask database instance at the specified path."
  @spec new(Path.t()) :: {:ok, t()} | {:error, any()}
  def new(path) do
    with :ok <- :filelib.ensure_dir(path),
         {:ok, datafiles} <- Datafile.open(path),
         {:ok, keydir} <- Keydir.new(path, datafiles),
         active_datafile_id <- map_size(datafiles) + 1,
         {:ok, active_datafile} <-
           path |> Datafile.build_path(active_datafile_id) |> Datafile.new() do
      {:ok,
       %__MODULE__{
         path: path,
         keydir: keydir,
         active_file: active_datafile_id,
         file_handles: Map.put(datafiles, active_datafile_id, active_datafile)
       }}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Closes the store after we're done with it."
  @spec close(t()) :: :ok | {:error, any()}
  def close(store) do
    with :ok <- Keydir.persist(store.keydir, store.path),
         :ok <-
           store.file_handles
           |> Enum.reduce_while(:ok, fn {_, file_handle}, acc ->
             file_handle
             |> Datafile.close()
             |> case do
               :ok -> {:cont, acc}
               {:error, reason} -> {:halt, {:error, reason}}
             end
           end) do
      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Retrieves the value for a key from the store."
  @spec get(t(), Datafile.Entry.key_t()) :: Datafile.Entry.t() | nil
  def get(store, key) do
    with entry_location <- Keydir.get(store.keydir, key),
         false <- is_nil(entry_location),
         datafile <- Map.get(store.file_handles, entry_location.file_id),
         {:ok, entry} <- Datafile.get(datafile, entry_location.value_size) do
      entry
    else
      true -> nil
      {:error, _reason} -> nil
    end
  end

  @doc """
  Writes a key-value pair in the database with expiration. 

  A value of 0 denotes no expiration. Otherwise, expiration is a UNIX timestamp
  in milliseconds.
  """
  @spec put(t(), DataFile.Entry.key_t(), Datafile.Entry.value_t(), non_neg_integer()) ::
          {:ok, t()} | {:error, any()}
  def put(store, key, value, expiration) do
    active_datafile = Map.get(store.file_handles, store.active_file)

    active_datafile
    |> Datafile.write(key, value, expiration)
    |> case do
      {:ok, updated_datafile} ->
        updated_file_handles = Map.put(store.file_handles, store.active_file, updated_datafile)

        updated_keydir =
          Keydir.put(store.keydir, key, %{
            file_id: store.active_file,
            value_pos: active_datafile.offset,
            value_size: updated_datafile.offset - active_datafile.offset
          })

        {:ok, %{store | file_handles: updated_file_handles, keydir: updated_keydir}}

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
    |> Enum.reduce({store, 0}, fn key, {store_acc, deleted_keys_count} ->
      tombstone_value = Datafile.Entry.deleted_sentinel()

      store_acc
      |> put(key, tombstone_value, 0)
      |> case do
        {:ok, updated_store} ->
          {updated_store, deleted_keys_count + 1}

        {:error, _reason} ->
          {store_acc, deleted_keys_count}
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
  @spec merge(t()) :: {:ok, t()} | {:error, term()}
  def merge(store) when map_size(store.file_handles) > 2 do
    merge_dir = store.path |> Path.join("merge") |> to_charlist()
    merge_datafile_path = Datafile.build_path(merge_dir, 0)

    with :ok <- :file.make_dir(merge_dir),
         {:ok, merge_datafile} <- Datafile.new(merge_datafile_path),
         {:ok, merge_keydir} <- populate_valid_entries(store.file_handles, merge_datafile),
         :ok <- remove_stale_datafiles(store.path),
         :ok <- :file.rename(merge_dir, Datafile.build_path(store.path, 0)),
         :ok <- :file.del_dir_r(merge_dir),
         :ok <- Keydir.persist(merge_keydir, store.path),
         updated_store <- %{
           store
           | file_handles: %{0 => merge_datafile},
             active_file: 0,
             keydir: merge_keydir
         } do
      {:ok, updated_store}
    else
      {:error, reason} ->
        :file.del_dir_r(merge_dir)
        {:error, reason}
    end
  end

  def merge(store), do: {:ok, store}

  @doc """
  Log rotation, in Bitcask, is the process of creating new active datafile when
  certain conditions are met. 

  Beetle makes use of file size to kickoff log rotation i.e. if the active
  datafile size exceeds certain threshold, new active datafile will be created.
  Unlike traditional log rotation that might delete old files, Bitcask's
  rotation creates new files while preserving old ones, mainting an append only
  storage model. The older files are usually cleaned during compaction process.
  """
  @spec log_rotation(t()) :: {:ok, t()} | {:error, any()}
  def log_rotation(store) do
    new_file_id = store.active_file + 1

    store.path
    |> Datafile.build_path(new_file_id)
    |> Datafile.new()
    |> case do
      {:ok, new_datafile} ->
        updated_file_handles = Map.put(store.file_handles, new_file_id, new_datafile)
        {:ok, %{store | active_file: new_file_id, file_handles: updated_file_handles}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc "Force any writes to sync to disk."
  @spec sync(t()) :: :ok
  def sync(store) do
    store.file_handles
    |> Map.get(store.active_file)
    |> Datafile.sync()
  end

  # === Private

  @spec populate_valid_entries(Datafile.map_t(), Datafile.t()) ::
          {:ok, Keydir.t()} | {:error, term()}
  defp populate_valid_entries(datafiles, merge_datafile) do
    datafiles
    |> Task.async_stream(fn {_, datafile} -> Datafile.scan_valid_entries(datafile) end,
      ordered: false,
      timeout: :timer.seconds(15),
      max_concurrency: System.schedulers_online() * 2
    )
    |> Enum.reduce_while({:ok, {merge_datafile, %{}}}, fn
      {:ok, entries_stream}, {:ok, {datafile, keydir}} ->
        entries_stream
        |> process_entry_batch(datafile, keydir)
        |> case do
          {:ok, {updated_datafile, updated_keydir}} ->
            {:cont, {:ok, {updated_datafile, updated_keydir}}}

          error ->
            {:halt, error}
        end

      {:exit, reason}, _ ->
        {:halt, {:error, {:scan_failed, reason}}}
    end)
    |> case do
      {:ok, {_, keydir}} -> {:ok, keydir}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec process_entry_batch(Enumerable.t(), Datafile.t(), Keydir.t()) ::
          {:ok, {Datafile.t(), Keydir.t()}} | {:error, term()}
  defp process_entry_batch(entries, datafile, keydir) do
    Enum.reduce_while(entries, {:ok, {datafile, keydir}}, fn entry, {:ok, {datafile, keydir}} ->
      datafile
      |> Datafile.write(entry.key, entry.value, entry.expiration)
      |> case do
        {:ok, updated_datafile} ->
          updated_keydir =
            Keydir.put(keydir, entry.key, %{
              file_id: 0,
              value_size: entry.size,
              value_pos: updated_datafile.offset - datafile.offset
            })

          {:cont, {:ok, {updated_datafile, updated_keydir}}}

        error ->
          {:halt, error}
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
