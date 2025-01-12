defmodule Beetle.Storage.Supervisor do
  @moduledoc """
  Supervises multiple Bitcask storage engine, each handling a shard of the
  keyspace. Provides automatic distribution of keys across shards using
  consistent hashing.
  """
  use Supervisor

  alias Beetle.Config

  # ==== Client

  def start_link(_), do: Supervisor.start_link(__MODULE__, nil, name: __MODULE__)

  # ==== Server

  @impl true
  def init(_) do
    shards = Config.database_shards()

    children =
      [
        {Registry, keys: :unique, name: Beetle.ShardRegistry},
        for shard_id <- 0..(shards - 1) do
          %{
            restart: :permanent,
            id: {:bitcask_shard, shard_id},
            start: {Beetle.Storage.Engine, :start_link, [shard_id]}
          }
        end
      ]
      |> List.flatten()

    Supervisor.init(children, strategy: :one_for_one)
  end
end
