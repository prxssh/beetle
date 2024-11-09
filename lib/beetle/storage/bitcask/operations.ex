defmodule Beetle.Storage.Bitcask.Operations do
  @moduledoc """
  Handler for different operations on the bitcask datastore.
  """
  alias Beetle.Storage.Bitcask.{
    Store,
    Keydir,
    Datafile
  }

  alias Beetle.Config.State, as: Config

  @doc """
  Retrieve a value by key from a Bitcask datastore.
  """
  @spec get(Store.t(), String.t()) :: {:ok, any()} | {:error, any()}
  def get(store, key) do
    fetch_datafile = fn file_id ->
      if store.file_id != file_id,
        do: store.stale_datafiles.file_id,
        else: store.active_datafile
    end

    with {:ok, {file_id, value_size, value_pos, _timestamp}} <- Keydir.get(store.keydir, key),
         datafile <- fetch_datafile.(file_id),
         {:ok, value} <- Datafile.get(datafile, value_size, value_pos) do
      {:ok, value}
    else
      error -> error
    end
  end

  @doc """
  Store a key and value in a Bitcask datastore.
  """
  @spec put(Store.t(), String.t(), any(), non_neg_integer()) :: {:ok, Store.t()} | {:error, any()}
  def put(store, key, value, expiration \\ 0) do
    with {:ok, offset} <- Datafile.put(store.active_datafile, key, value, expiration),
         {:ok, keydir} <-
           Keydir.put(
             store.keydir,
             key,
             store.file_id,
             store.active_datafile.offset,
             offset,
             NaiveDateTime.utc_now()
           ),
         datafile <- Map.put(store.active_datafile, :offset, offset) do
      {:ok, %{store | active_datafile: datafile, keydir: keydir}}
    else
      error -> error
    end
  end

  @doc """
  Delete a key from a Bitcask datastore.
  """
  @spec delete(Store.t(), String.t()) :: {:ok, Store.t()} | {:error, any()}
  def delete(store, key) do
    case put(store, key, nil) do
      {:ok, updated_store} -> {:ok, updated_store}
      error -> error
    end
  end

  @doc """
  List all keys in a Bitcask datastore.
  """
  @spec list_keys(Store.t()) :: {:ok, [String.t()]}
  def list_keys(store), do: {:ok, Map.keys(store.keydir)}

  @doc """
  Force any writes to sync to disk.
  """
  @spec sync(Store.t()) :: :ok | {:error, any()}
  def sync(store), do: Datafile.sync(store.active_datafile)

  @doc """
  Close a Bitcask datastore and flush all pending writes (if any) to disk.
  """
  @spec close(Store.t()) :: :ok | {:error, any()}
  def close(store) do
    with :ok <- Keydir.close(store.keydir),
         :ok <- Datafile.sync(store.active_datafile),
         :ok <- Datafile.close(store.active_datafile),
         :ok <-
           Enum.reduce_while(store.stale_datafiles, :ok, fn {_, datafile}, acc ->
             case Datafile.close(datafile) do
               :ok -> {:cont, acc}
               {:error, reason} -> {:halt, {:error, reason}}
             end
           end) do
      :ok
    else
      error -> error
    end
  end

  @doc """
  Merge several data files within a Bitcask datastore into a more compact form.
  Also, produces hintfiles for faster startups.
  """
  @spec merge(Store.t()) :: :ok | {:error, any()}
  def merge(store) do
  end

  @doc """
  Creates a new data file after the currently active data file size limit is
  breached.
  """
  @spec log_rotation(Store.t()) :: {:ok, Store.t()} | {:error, any()}
  def log_rotation(store) do
    with {:ok, current_size} <- Datafile.get_file_size(store.active_datafile),
         false <- current_size < Config.get_log_file_size(),
         {:ok, new_datafile} <- Datafile.new(store.file_id + 1, Config.get_storage_directory()),
         stale_datafiles <- %{store.stale_datafiles | store.file_id => store.active_datafile} do
      {:ok,
       %{
         store
         | active_datafile: new_datafile,
           stale_datafiles: stale_datafiles,
           file_id: store.file_id + 1
       }}
    else
      true -> {:ok, store}
      error -> error
    end
  end
end
