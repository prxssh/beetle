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

  alias Beetle.Config

  alias Beetle.Storage.Bitcask.{
    Keydir,
    Datafile
  }

  @type file_id_t :: non_neg_integer()
  @type file_handle_t :: %{file_id_t() => Datafile.t()}

  @type t :: %__MODULE__{
          keydir: Keydir.t(),
          active_file: file_id_t(),
          file_handles: file_handle_t()
        }

  defstruct(
    keydir: nil,
    active_file: 0,
    file_handles: nil
  )

  @doc """
  Creates a new Bitcask instance at the storage directory.
  """
  @spec new() :: {:ok, t()} | {:error, any()}
  def new do
    with path <- Config.storage_directory(),
         {:ok, datafile_handles} <- Datafile.open_datafiles(path),
         {:ok, keydir} <- Keydir.new(path, datafile_handles),
         active_datafile_id <- map_size(datafile_handles) + 1,
         {:ok, active_datafile_handle} <-
           path |> Datafile.get_name(active_datafile_id) |> Datafile.new() do
      {:ok,
       %__MODULE__{
         keydir: keydir,
         active_file: active_datafile_id,
         file_handles: Map.put(datafile_handles, active_datafile_id, active_datafile_handle)
       }}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Closes the store after we're done with it.

  It does the following tasks:
  - Persists keydir to disk
  - Sync any pending writes to disk
  - Close all file handles
  """
  @spec close(t()) :: :ok | {:error, any()}
  def close(store) do
    with :ok <- Keydir.persist(store.keydir),
         :ok <- store.file_handles |> Map.get(store.active_file) |> Datafile.sync(),
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

  @doc """
  Retrieve a value by key from the store. 

  Returns `nil` if the value is not found, or expired.
  """
  @spec get(t(), Datafile.Entry.key_t()) :: {:ok, Datafile.Entry.value_t()} | nil
  def get(store, key) do
    with entry_location <- store.keydir |> Keydir.get(key),
         true <- not is_nil(entry_location),
         {:ok, value} <-
           store.file_handles
           |> Map.get(entry_location.file_id)
           |> Datafile.get(entry_location.value_pos) do
      value
    else
      false ->
        nil

      {:error, reason} ->
        Logger.notice("#{__MODULE__}.get/2: #{inspect(reason)}")
        nil
    end
  end

  @doc """
  Write a key-value pair in the store with additional options.

  Currently, the only supported additonal option is `expiration`.
  """
  @spec put(Bitcask.t(), DataFile.Entry.key_t(), Datafile.Entry.value_t(), non_neg_integer()) ::
          {:ok, t()} | {:error, any()}
  def put(store, key, value, expiration) do
    active_datafile = Map.get(store.file_handles, store.active_file)

    active_datafile
    |> Datafile.write(key, value, expiration)
    |> case do
      {:ok, updated_datafile} ->
        updated_file_handles = Map.put(store.file_handles, store.active_file, updated_datafile)
        updated_keydir = Keydir.put(store.keydir, key, store.active_file, active_datafile.offset)

        {:ok, %{store | file_handles: updated_file_handles, keydir: updated_keydir}}

      error ->
        error
    end
  end

  @doc """
  Delete key(s) from the store.

  This operation doesn't immediately removes the key but overwrites it with a
  tombstone value. The deleted keys are cleaned up during merging operation. It
  returns a non-negative integer specifying the count of keys deleted.
  """
  @spec delete(t(), [Datafile.Entry.key_t()] | Datafile.Entry.key_t()) :: {t(), non_neg_integer()}
  def delete(store, keys) do
    keys
    |> List.wrap()
    |> Enum.reduce({store, 0}, fn key, {store_acc, deleted_keys_count} ->
      tombstone_value = Datafile.Entry.deleted_sentinel()

      store_acc
      |> put(key, tombstone_value, 0)
      |> case do
        {:ok, updated_store} ->
          {updated_store, deleted_keys_count + 1}

        {:error, reason} ->
          Logger.notice("#{__MODULE__} failed to delete key #{key}: #{inspect(reason)}")
          {store_acc, deleted_keys_count}
      end
    end)
  end

  @doc "List all the keys in the store"
  @spec keys(t()) :: [Datafile.Entry.key_t()] | {:error, any()}
  def keys(store), do: Map.keys(store.keydir)

  @doc """
  Merges several datafiles within the store into a more compact form.

  This function also takes care of cleaning up the expired and deleted entries
  from the store.
  """
  @spec merge(t()) :: {:ok, t()} | {:error, any()}
  def merge(store) when map_size(store.file_handles) > 1 do
    merge_dir = Config.storage_directory() |> Path.join("merge")
    new_datafile_path = merge_dir |> Datafile.get_name(0)

    with :ok <- :file.make_dir(merge_dir),
         {:ok, merged_file} <- Datafile.new(new_datafile_path),
         {:ok, {merged_datafile, merged_keydir}} <-
           populate_new_entries(store.file_handles, merged_file),
         {:ok, merged_store} <- recreate_store(new_datafile_path, merged_datafile, merged_keydir),
         :ok <- close(store),
         :ok <- Keydir.persist(merged_keydir),
         :ok <- :file.del_dir_r(merge_dir) do
      {:ok, merged_store}
    else
      {:error, reason} ->
        :file.del_dir_r(merge_dir)
        {:error, reason}
    end
  end

  def merge(store), do: {:ok, store}

  @doc """
  Creates a new log file (datafile) for the store. 

  The previous log file will no longer be used for writing, just for reading.
  """
  @spec log_rotation(t()) :: {:ok, t()} | {:error, any()}
  def log_rotation(store) do
    new_file_id = store.active_file + 1

    Config.storage_directory()
    |> Datafile.get_name(new_file_id)
    |> Datafile.new()
    |> case do
      {:ok, new_active_log} ->
        updated_file_handles = Map.put(store.file_handles, new_file_id, new_active_log)
        {:ok, %{store | active_file: new_file_id, file_handles: updated_file_handles}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc "Force any writes to sync to disk."
  @spec sync(t()) :: :ok
  def sync(store) do
    store
    |> Map.get(:file_handles, store.active_file)
    |> Datafile.sync()
  end

  # === Private

  # Write all the active entries (unexpired, and undeleted) to the new datafile
  # and keydir.
  @spec populate_new_entries(file_handle_t(), Datafile.t()) ::
          {:ok, {Datafile.t(), Keydir.t()}} | {:error, any()}
  defp populate_new_entries(older_logs, merged_log) do
    with {:ok, entries} <- collect_entries(older_logs),
         {:ok, {final_file, final_keydir}} <- write_entries(entries, merged_log) do
      {:ok, {final_file, final_keydir}}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  # Aggregates all the active entries from the older datafiles.
  @spec collect_entries(file_handle_t()) :: {:ok, [Datafile.Entry.t()]} | {:error, any()}
  defp collect_entries(older_logs) do
    older_logs
    |> Enum.reduce_while({:ok, []}, fn {_, datafile}, {:ok, acc} ->
      datafile.reader
      |> Datafile.Entry.dump_all(0, datafile.offset)
      |> case do
        {:ok, entries} -> {:cont, {:ok, acc ++ Enum.map(entries, fn {_, entry} -> entry end)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  # Writes the active entries to the new datafile and keydir.
  @spec write_entries([Datafile.Entry.t()], Datafile.t()) ::
          {:ok, {Datafile.t(), Keydir.t()}} | {:error, any()}
  defp write_entries(entries, merged_datafile) do
    entries
    |> Enum.reduce_while({:ok, {merged_datafile, %{}}}, fn entry, {:ok, {datafile, keydir}} ->
      datafile
      |> Datafile.write(entry.key, entry.value, entry.expiration)
      |> case do
        {:ok, updated_datafile} ->
          updated_keydir = Keydir.put(keydir, entry.key, 0, datafile.offset)
          {:cont, {:ok, {updated_datafile, updated_keydir}}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
  end

  @spec recreate_store(Path.t(), Datafile.t(), Keydir.t()) :: {:ok, t()} | {:error, any()}
  defp recreate_store(curr_path, merged_datafile, merged_keydir) do
    dest_path = Config.storage_directory() |> Datafile.get_name(0)

    with :ok <- Datafile.close(merged_datafile),
         :ok <- :file.rename(curr_path, dest_path),
         {:ok, datafile} <- Datafile.new(dest_path) do
      {:ok,
       %__MODULE__{
         active_file: 0,
         keydir: merged_keydir,
         file_handles: %{0 => datafile}
       }}
    else
      {:error, reason} -> {:error, reason}
    end
  end
end
