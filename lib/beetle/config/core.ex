defmodule Beetle.Config do
  @moduledoc """
  Configuration manager for the Beetle database.

  This module provides a centralized way to access configuration settings for
  the database. It uses Elixir's [Agent](https://hexdocs.pm/elixir/Agent.html)
  for maintaining state and provides simple accessor functions for retrieving
  configuration values.

  ## Usage

  Start the configuration manager with a path to the config file:

  ```elixir
  {:ok, _pid} = Beetle.Config.start_link("/path/to/config.exs")
  ```

  Then access configuration values using the provided functions:

  ```elixir
  port = Beetle.Config.port()
  storage_path = Beetle.Config.storage_directory()
  ```
  The configuration is loaded once at startup and maintained in an Agent process
  under the application supervisor.
  """
  use Agent
  alias Beetle.Config.Parser

  @module __MODULE__

  # === Client

  def start_link(path) do
    parsed_config = Parser.read_config(path)
    Agent.start_link(fn -> parsed_config end, name: @module)
  end

  @doc "Configured port number for the database server."
  def port, do: Agent.get(@module, & &1.port)

  @doc "Maximum allowed size of individual log files (in bytes)."
  def log_file_size, do: Agent.get(@module, & &1.log_file_size)

  @doc "Time interval (in seconds) between automatic data merges."
  def merge_interval, do: Agent.get(@module, & &1.merge_interval)

  @doc "Number of database shards for distributed storage."
  def database_shards, do: Agent.get(@module, & &1.database_shards)

  @doc "Path where database files are stored."
  def storage_directory, do: Agent.get(@module, & &1.storage_directory)

  @doc "Time interval (in seconds) between log file rotations."
  def log_rotation_interval, do: Agent.get(@module, & &1.log_rotation_interval)
end
