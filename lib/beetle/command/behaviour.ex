defmodule Beetle.Command.Behaviour do
  @moduledoc """
  Defines behaviour for Beetle command handlers.

  Command handlers must implement the `handle/2` callback to process the
  commands. Each command type (String, Hash, List etc.) has its own handler
  module implementing this behaviour.
  """

  @doc """
  Handles a Redis command with its arguments.

  Parameters:
  - command: Uppercase Redis command name (e.g. "GET", "SET")
  - args: List of command arguments

  Returns:
  - Success: Term representing command result 
  - Error: {:error, reason} tuple with error message
  """
  @callback handle(command :: String.t(), args :: [String.t()]) :: term() | {:error, String.t()}
end
