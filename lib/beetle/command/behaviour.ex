defmodule Beetle.Command.Behaviour do
  @moduledoc """
  Defines behaviour for Beetle command handlers.

  Command handlers must implement the `handle/2` callback to process the
  commands. Each command type (String, Hash, List etc.) has its own handler
  module implementing this behaviour.
  """

  @doc """
  Handles a Beetle command with its arguments.

  Parameters:
  - command: Uppercase Beetle command name (e.g. "GET", "SET")
  - args: List of command arguments

  Returns:
  - Success: Term representing command result 
  - Error: {:error, reason} tuple with error message
  """
  @callback handle(command :: String.t(), args :: [String.t()]) :: term() | {:error, String.t()}

  @doc """
  Handles a Beetle command with transaction context.
  Optional callback for transaction-aware commands.

  Parameters:
  - command: Uppercase Beetle command name (e.g. "MULTI", "EXEC")
  - args: List of command arguments
  - context: Map containing transaction state and context information

  Returns:
  - Success: Term representing command result
  - Error: {:error, reason} tuple with error message 
  """
  @callback with_context(
              ctx :: Beetle.Transaction.t(),
              command :: String.t(),
              args :: [String.t()]
            ) :: {term(), Beetle.Transaction.t()}

  @optional_callbacks [with_context: 3, handle: 2]
end
