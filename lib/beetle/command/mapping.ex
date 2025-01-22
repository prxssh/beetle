defmodule Beetle.Command.Mapping do
  @moduledoc """
  Mapping of commands to their respective modules
  """
  alias Beetle.Command.Types

  @commands %{
    # Miscellaneous
    PING: Types.Misc,
    TTL: Types.Misc,
    # String
    GET: Types.String,
    SET: Types.String,
    DEL: Types.String,
    APPEND: Types.String,
    GETDEL: Types.String,
    GETEX: Types.String,
    GETRANGE: Types.String,
    STRLEN: Types.String,
    DECR: Types.String,
    DECRBY: Types.String,
    INCR: Types.String,
    INCRYBY: Types.String
  }

  @doc "Gets the module responsible for handling a specific command"
  @spec get(String.t()) :: {:ok, module()} | {:error, String.t()}
  def get(command) do
    case Map.get(@commands, command) do
      nil -> {:error, "ERR unkown command '#{command}'"}
      module -> {:ok, module}
    end
  end
end
