defmodule Beetle.Config do
  @moduledoc """
  Configuration Manager for the Beetle database.

  This module provides a centrailzed way of accessing database configuration.
  It uses Elixir's [Agent](https://hexdocs.pm/elixir/Agent.html#content) for
  maintaing state and provides simple accessor functions for retrieving
  configuration values.

  ## Usage

  Start the configuration manager with a path to the config file under the
  supervisor.

  ```elixir
  children = [
    {Beetle.Config, "path/to/beetle.conf"}
  ]
  Supervisor.start_link(children, strategy: :one_for_one)
  ```

  Then we can access the configuration values using the provided functions:

  ```elixir
  port = Beetle.Config.port()
  storage_directory = Beetle.Config.storage_directory()
  ```

  This configuration is loaded once at startup and maintained in an Agent
  process under application supervisor.
  """
  use Agent
  alias Beetle.Config.Parser, as: ConfigParser

  def start_link(path) do
    config = ConfigParser.read_config(path)
    Agent.start_link(fn -> config end, name: __MODULE__)
  end

  @doc "Configured port for the database server."
  @spec port() :: pos_integer()
  def port, do: Agent.get(__MODULE__, & &1.port)

  @doc "Maximum allowed size of log file (in bytes)."
  @spec log_file_size() :: pos_integer()
  def log_file_size, do: Agent.get(__MODULE__, & &1.log_file_size)

  @doc "Time interval (in seconds) between database merge operation."
  @spec merge_interval() :: pos_integer() | nil
  def merge_interval, do: Agent.get(__MODULE__, & &1.merge_interval)

  @doc "Time interval (in seconds) between database log rotation operation."
  @spec log_rotation_interval() :: pos_integer() | nil
  def log_rotation_interval, do: Agent.get(__MODULE__, & &1.log_rotation_interval)

  @doc "Number of shards configured for database."
  @spec database_shards() :: pos_integer()
  def database_shards, do: Agent.get(__MODULE__, & &1.database_shards)

  @doc "Directory where database files are stored."
  @spec storage_directory() :: String.t()
  def storage_directory, do: Agent.get(__MODULE__, & &1.storage_directory)
end
