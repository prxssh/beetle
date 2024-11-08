defmodule Beetle.Storage.Supervisor do
  @moduledoc """
  Supervisor for Bitcask storage engine and its worker processes.
  """
  use Supervisor

  alias Beetle.Storage.{
    Workers,
    Engine
  }

  @typedoc """
  Configuration options for the storage engine.

  Required fields:
  * `storage_dir` - Directory path where all data files will be stored
  * `sync_interval` - Time in seconds between disk sync operations
  * `compaction_interval` - Time in seconds between compaction runs
  * `log_rotation_interval` - Time in seconds between log rotation checks
  """
  @type config_opts_t :: %{
          storage_dir: String.t(),
          sync_interval: pos_integer(),
          compaction_interval: pos_integer(),
          log_rotation_interval: pos_integer()
        }

  @module __MODULE__

  # === Client

  def start_link(config_opts), do: Supervisor.start_link(@module, config_opts, name: @module)

  # === Server

  @impl true
  def init(config_opts) do
    children = [
      {Worker, config_opts},
      {Engine, config_opts}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
