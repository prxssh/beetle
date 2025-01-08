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

  def storage_directory, do: Agent.get(@module, & &1.storage_directory)

  def port, do: Agent.get(@module, & &1.port)
end
