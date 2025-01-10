defmodule Beetle.Storage.Bitcask.Operations do
  @moduledoc """
  Implements core operations for the Bitcask storage engine including reads,
  writes, and log management operations.

  This module provides the low-level operations that forms the foundation of
  the Bitcask storage engine, handling the actual reading and writing of data
  to disk and mangaing datafiles.
  """
  alias Beetle.Storage.Bitcask.{
    Store,
    Datafile
  }

  @type key_t :: String.t()
  @type value_t :: binary() | list() | map()

  @type opts_t :: %{
          expiration: non_neg_integer()
        }

  @default_opts %{
    expiration: 0
  }

  @doc """
  Retrieve a value by key from the store. 

  Returns `nil` if the value is not found, or expired.
  """
  @spec get(Store.t(), key_t()) :: {:ok, value_t()} | nil
  def get(store, key) do
  end

  @doc """
  Stores a key and value in the store with additional options.

  Currently, the only supported option is `expiration`.
  """
  @spec put(Store.t(), key_t(), value_t(), opts_t()) :: :ok | {:error, any()}
  def put(store, key, value, opts \\ @default_expiration) do
  end

  @doc """
  Delete key(s) from the store.

  This operation doesn't immediately removes the key but overwrites it with a
  tombstone value. The deleted keys are cleaned up during merging operation. It
  returns a non-negative integer specifying the count of keys deleted.
  """
  @spec delete(Store.t(), [key_t()] | key_t()) :: non_neg_integer() | {:error, any()}
  def delete(store, keys) do
  end

  @doc "List all the keys in the store"
  @spec keys(Store.t()) :: [key_t()] | {:error, any()}
  def keys(store), do: Map.keys(store.keydir)

  @doc """
  Merges several datafiles within the store into a more compact form.

  This function also takes care of cleaning up the expired and deleted entries
  from the store.
  """
  @spec merge(Store.t()) :: :ok | {:error, any()}
  def merge(store) do
  end

  @doc """
  Creates a new log file (datafile) for the store. 

  The previous log file will no longer be used for writing, just for reading.
  """
  @spec log_rotation(Store.t()) :: :ok | {:error, any()}
  def log_rotation(store) do
  end

  @doc "Force any writes to sync to disk."
  @spec sync(Store.t()) :: :ok
  def sync(store) do
    store
    |> Map.get(:file_handles, store.active_file)
    |> Datafile.sync()
  end

  @doc "Closes the store and flush all pending writes"
  @spec close(Store.t()) :: :ok
  def close(store), do: Store.close(store)
end
