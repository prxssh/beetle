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

  alias Beetle.Config
  alias Beetle.Storage.Bitcask

  # ==== Client

  def start_link(shard_id),
    do: GenServer.start_link(__MODULE__, shard_id, name: via_tuple(shard_id))

  @spec get(String.t()) :: Datafile.Entry.t() | nil
  def get(key) do
    key
    |> get_shard()
    |> via_tuple()
    |> GenServer.call({:get, key})
  end

  @spec get_value(String.t()) :: nil | Datafile.Entry.value_t()
  def get_value(key) do
    key
    |> get()
    |> case do
      nil -> nil
      %{value: value} -> value
    end
  end

  @spec put(String.t(), term(), non_neg_integer()) :: :ok
  def put(key, value, expiration) do
    key
    |> get_shard()
    |> via_tuple()
    |> GenServer.cast({:put, key, value, expiration})
  end

  @spec drop([String.t()]) :: non_neg_integer()
  def drop(keys) do
    keys
    |> List.wrap()
    |> Enum.group_by(&get_shard/1)
    |> Enum.reduce(0, fn {shard_id, keys}, acc ->
      count = GenServer.call(via_tuple(shard_id), {:drop, keys})
      acc + count
    end)
  end

  # ==== Server

  @impl true
  def init(shard_id) do
    path = Config.storage_directory() |> Path.join("shard_#{shard_id}") |> Kernel.<>("/")

    path
    |> Bitcask.new()
    |> case do
      {:ok, store} -> {:ok, store}
      error -> {:halt, error}
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
    {:ok, updated_store} = Bitcask.put(store, key, value, expiration)
    {:noreply, updated_store}
  end

  # ==== Private

  defp via_tuple(shard_id), do: {:via, Registry, {Beetle.ShardRegistry, shard_id}}

  defp get_shard(key) do
    count_shards = Config.database_shards()
    :erlang.phash2(key, count_shards)
  end
end
