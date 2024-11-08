defmodule Beetle.Config.State do
  @moduledoc """
  Configuration manager for beetle. It allows sharing of state between process
  using Agent.
  """
  use Agent, restart: :transient, shutdown: 10_000

  alias Beetle.Config.Parser

  @module __MODULE__

  def start_link(config_path) do
    dbg(config_path)

    config_path
    |> Parser.load()
    |> case do
      {:ok, config} -> Agent.start_link(fn -> config end, name: @module)
      {:error, reason} -> raise "Failed to load config: #{inspect(reason)}"
    end
  end

  def get_port, do: get(:port)
  def get_host, do: get(:host)
  def get_sync_interval, do: get(:sync_interval)
  def get_log_file_size, do: get(:log_file_size)
  def get_merge_interval, do: get(:merge_interval)
  def get_storage_directory, do: get(:storage_directory)

  defp get(key), do: Agent.get(__MODULE__, &Map.get(&1, key))
end
