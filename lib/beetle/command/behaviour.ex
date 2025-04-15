defmodule Beetle.Command.Behaviour do
  @moduledoc """
  Defines behaviour for Beetle command handlers.

  Command handlers must implement the `handle/2` callback to process the
  commands. Each command type (String, Hash, List etc.) has its own handler
  module implementing this behaviour.
  """

  @doc "Handles a Beetle command with its arguments."
  @callback handle(ctx :: Beetle.Command.context_t(), cmd :: String.t(), args :: [String.t()]) ::
              {term(), Beetle.Command.context_t()}
end
