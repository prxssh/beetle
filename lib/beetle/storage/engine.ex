defmodule Beetle.Storage.Engine do
  @moduledoc """
  Engine to manages persistent data across multiple shards.

  This module implements a sharded key-value store using Bitcask as the
  underlying storage engine. It provides a simple interface for CRUD operations
  while handling data distribution across shards transparently.

  Data is automatically distributed across multiple shards using consistent
  hashing (`:erlang.phash2/2`). The number of shards is configurable through
  the `Beetle.Config.database_shards/0`. Each shard maintains its own Bitcask
  store in a separate directory under the configured storage path.
  """
  use GenServer

  require Logger
  alias Beetle.Config
  alias Beetle.Storage.Bitcask

  ########## Client

  def start_link(shard_id),
    do: GenServer.start_link(__MODULE__, shard_id, name: via_tuple(shard_id))

  @spec get_value(String.t()) :: term() | {:error, String.t()}
  def get_value(key) do
    case get(key) do
      {:ok, nil} -> nil
      {:ok, %Bitcask.Datafile.Entry{value: value}} -> value
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Get the value stored at key in the database.
  """
  @spec get(String.t()) :: {:ok, Bitcask.Datafile.Entry.t() | nil} | {:error, String.t()}
  def get(key) do
    key
    |> get_shard()
    |> via_tuple()
    |> GenServer.call({:get, key})
  end

  @doc """
  Write a new key-value pair to the database with optional expiration.

  KV pair is not persisted immediately, and will take some time to reflect in
  the database since we're syncing writes every 2s.
  """
  @spec put(String.t(), term(), non_neg_integer()) :: :ok
  def put(key, value, expiration \\ 0) do
    key
    |> get_shard()
    |> via_tuple()
    |> GenServer.cast({:put, key, value, expiration})
  end

  @doc """
  Delete(s) keys from the database.

  Returns the count of deleted keys.
  """
  @spec drop(String.t() | [String.t()]) :: non_neg_integer()
  def drop(keys) do
    keys
    |> List.wrap()
    |> Enum.group_by(&get_shard/1)
    |> Enum.reduce(0, fn {shard_id, keys}, acc ->
      count = GenServer.call(via_tuple(shard_id), {:drop, keys})
      acc + count
    end)
  end

  ########## Server

  @impl true
  def init(shard_id) do
    path = Path.join(Config.storage_directory(), "shard_#{shard_id}/")

    case Bitcask.new(path) do
      {:ok, store} ->
        schedule_compaction()
        schedule_log_rotation()

        Logger.debug("#{__MODULE__} started bitcask shard #{shard_id} successfully!")

        {:ok, store}

      {:error, reason} ->
        Logger.error(
          "#{__MODULE__} failed to start bitcask shard #{shard_id}, error: #{inspect(reason)}"
        )

        {:stop, {:error, reason}}
    end
  end

  @impl true
  def handle_call({:get, key}, _, store), do: {:reply, Bitcask.get(store, key), store}

  @impl true
  def handle_call({:drop, keys}, _, store) do
    {updated_store, count_deleted} = Bitcask.delete(store, keys)

    {:reply, count_deleted, updated_store}
  end

  @impl true
  def handle_cast({:put, key, value, expiration}, store) do
    {:ok, updated_store} = Bitcask.put(store, key, value, expiration: expiration)

    {:noreply, updated_store}
  end

  @impl true
  def handle_info(:log_rotation, store) do
    case Bitcask.log_rotation(store) do
      {:ok, updated_store} ->
        Logger.debug(
          "#{__MODULE__} log rotation performed successfully for database at path: #{store.path}"
        )

        {:noreply, updated_store}

      {:error, reason} ->
        Logger.error("#{__MODULE__} failed to perform log rotation: #{inspect(reason)}")
        {:noreply, store}
    end
  end

  @impl true
  def handle_info(:compaction, store) do
    case Bitcask.compaction(store) do
      {:ok, updated_store} ->
        Logger.debug(
          "#{__MODULE__} compaction performed successfully for database at path: #{store.path}"
        )

        {:noreply, updated_store}

      {:error, reason} ->
        Logger.error("#{__MODULE__} failed to perform compaction: #{inspect(reason)}")
        {:noreply, store}
    end
  end

  ########## Private

  defp via_tuple(shard_id), do: {:via, Registry, {Beetle.ShardRegistry, shard_id}}

  defp get_shard(key) do
    count_shards = Config.database_shards()
    :erlang.phash2(key, count_shards)
  end

  defp schedule_log_rotation do
    interval_ms = Config.log_rotation_interval()
    Process.send_after(self(), :log_rotation, interval_ms)
  end

  defp schedule_compaction do
    interval_ms = Config.merge_interval()
    Process.send_after(self(), :compaction, interval_ms)
  end
end
