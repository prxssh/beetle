defmodule Beetle.Command.Mapping do
  @moduledoc """
  Mapping of commands to their respective modules
  """
  alias Beetle.Command.Types

  @commands %{
    # Miscellaneous
    "PING" => Types.Misc,
    "TTL" => Types.Misc,
    # String
    "GET" => Types.String,
    "SET" => Types.String,
    "DEL" => Types.String,
    "APPEND" => Types.String,
    "GETDEL" => Types.String,
    "GETEX" => Types.String,
    "GETRANGE" => Types.String,
    "STRLEN" => Types.String,
    "DECR" => Types.String,
    "DECRBY" => Types.String,
    "INCR" => Types.String,
    "INCRBY" => Types.String,
    # Transaction
    "MULTI" => Types.Transaction,
    "EXEC" => Types.Transaction,
    "DISCARD" => Types.Transaction
  }

  def get(command) do
    case @commands[command] do
      nil -> {:error, "ERR unknown command '#{command}'"}
      Types.Transaction -> {:with_context, Types.Transaction}
      module -> {:handle, module}
    end
  end
end
