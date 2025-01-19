defmodule Beetle.Config do
  @moduledoc """
  Config Manager for the beetle database.
  """
  use Agent

  alias Beetle.Config.Parser

  @module __MODULE__

  # === Client

  def start_link(path) do
    parsed_config = Parser.read_config(path)
    Agent.start_link(fn -> parsed_config end, name: @module)
  end

  def port, do: Agent.get(@module, & &1.port)
  def log_file_size, do: Agent.get(@module, & &1.log_file_size)
  def merge_interval, do: Agent.get(@module, & &1.merge_interval)
  def database_shards, do: Agent.get(@module, & &1.database_shards)
  def storage_directory, do: Agent.get(@module, & &1.storage_directory)
  def log_rotation_interval, do: Agent.get(@module, & &1.log_rotation_interval)
end
