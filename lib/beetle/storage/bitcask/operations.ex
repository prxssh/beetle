defmodule Beetle.Storage.Bitcask.Operations do
  @moduledoc """
  Handler for different operations on the bitcask datastore.
  """
  alias Beetle.Storage.Bitcask.{
    Store,
    Keydir,
    Datafile
  }

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
  @spec put(Store.t(), String.t(), any()) :: {:ok, Store.t()} | {:error, any()}
  def put(store, key, value) do
    with {:ok, offset} <- Datafile.put(store.active_datafile, key, value),
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
  @spec delete(Store.t(), String.t()) :: :ok | {:error, any()}
  def delete(store, key) do
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
  @spec merge(String.t()) :: :ok | {:error, any()}
  def merge(dir) do
  end

  @doc """
  Creates a new data file after the currently active data file size limit is
  breached.
  """
  @spec log_rotation(String.t()) :: :ok | {:error, any()}
  def log_rotation(dir) do
  end
end
