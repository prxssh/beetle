defmodule Beetle.Command.Mapping do
  @moduledoc """
  Mapping of commands to their respective modules
  """
  alias Beetle.Command.Types.ConnectionManagment

  @commands %{
    "PING" => ConnectionManagment
  }

  @spec get(String.t()) :: {:ok, module()} | {:error, String.t()}
  def get(cmd) do
    case @commands[cmd] do
      nil -> {:error, "ERR unknown command '#{cmd}'"}
      module -> {:ok, module}
    end
  end
end
