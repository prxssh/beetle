defmodule Beetle.Storage.Engine do
  @moduledoc false
  use Agent

  require Logger

  alias Beetle.Config
  alias Beetle.Storage.Bitcask

  # ==== Client

  def start_link(shard_id) do
    storage_path = Config.storage_directory() |> Path.join("shard_#{shard_id}")

    Agent.start_link(
      fn ->
        {:ok, store} = Bitcask.new(storage_path)
        store
      end,
      name: via_tuple(shard_id)
    )
  end

  @spec get(String.t()) :: Datafile.Entry.t() | nil
  def get(key) do
    key
    |> get_shard()
    |> via_tuple()
    |> Agent.get(fn store -> store |> Bitcask.get(key) end)
  end

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
    |> Agent.update(fn store ->
      {:ok, updated_store} = Bitcask.put(store, key, value, expiration)
      updated_store
    end)
  end

  @spec drop([String.t()]) :: non_neg_integer()
  def drop(keys) do
    keys
    |> List.wrap()
    |> Enum.group_by(&get_shard/1)
    |> Enum.map(fn {shard_id, shard_keys} ->
      Agent.get_and_update(via_tuple(shard_id), fn store ->
        {updated_store, count_deleted} = Bitcask.delete(store, shard_keys)
        {count_deleted, updated_store}
      end)
    end)
    |> Enum.sum()
  end

  # ==== Private

  defp via_tuple(shard_id), do: {:via, Registry, {Beetle.ShardRegistry, shard_id}}

  defp get_shard(key) do
    count_shards = Config.database_shards()
    :erlang.phash2(key, count_shards)
  end
end
