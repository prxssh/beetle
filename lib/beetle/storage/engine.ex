defmodule Beetle.Storage.Engine do
  @moduledoc """
  A sharded key-value storage engine built on top of Bitcask.

  This module implements a distributed storage system that automatically
  partitions data across multiple Bitcask instances using consistent hashing.
  Each shard operates as an independent Bitcask store, handling a subset of the
  total keyspace.
  """
  use GenServer

  require Logger

  alias Beetle.Config
  alias Beetle.Storage.Bitcask

  @module __MODULE__

  # === Client 

  def start_link(shard_id), do: GenServer.start_link(@module, shard_id, name: via_tuple(shard_id))

  def get(key) do
    key
    |> get_shard()
    |> then(&GenServer.call(via_tuple(&1), {:get, key}))
  end

  def put(key, value, expiration) do
    key
    |> get_shard()
    |> then(&GenServer.call(via_tuple(&1), {:put, key, value, expiration}))
  end

  def delete(keys) do
    keys
    |> List.wrap()
    |> Enum.group_by(&get_shard/1)
    |> Enum.reduce(0, fn {shard_id, shard_keys}, total_deleted ->
      count = GenServer.call(via_tuple(shard_id), {:delete, shard_keys})
      total_deleted + count
    end)
  end

  # === Server

  @impl true
  def init(shard_id) do
    # Logger.notice("#{__MODULE__}: starting bitcask on shard #{shard_id}")

    Config.storage_directory()
    |> Path.join("shard_#{shard_id}")
    |> Bitcask.new()
    |> case do
      {:ok, store} -> {:ok, {store, shard_id}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:get, key}, _from, {store, shard_id}) do
    # Logger.notice("#{__MODULE__}: get '#{key}' on shard #{shard_id}")

    {:reply, Bitcask.get(store, key), {store, shard_id}}
  end

  @impl true
  def handle_call({:put, key, value, expiration}, _from, {store, shard_id}) do
    # Logger.notice("#{__MODULE__}: put '#{key}-#{inspect(value)}' on shard #{shard_id}")

    store
    |> Bitcask.put(key, value, expiration)
    |> case do
      {:ok, updated_store} -> {:reply, :ok, {updated_store, shard_id}}
      {:error, reason} -> {:reply, {:error, reason}, {store, shard_id}}
    end
  end

  @impl true
  def handle_call({:delete, keys}, _from, {store, shard_id}) do
    {updated_store, count_deleted_keys} = Bitcask.delete(store, keys)
    {:reply, count_deleted_keys, {updated_store, shard_id}}
  end

  # === Private

  defp via_tuple(shard_id), do: {:via, Registry, {Beetle.ShardRegistry, shard_id}}

  defp get_shard(key) do
    count_shards = Config.database_shards()
    :erlang.phash2(key, count_shards)
  end
end
