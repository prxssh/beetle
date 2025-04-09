defmodule Beetle.Utils do
  @moduledoc """
  Helper utils to make life easier
  """

  @doc "Converts a string to integer"
  @spec parse_integer(str :: String.t()) :: {:ok, integer()} | {:error, String.t()}
  def parse_integer(str) do
    case Integer.parse(str) do
      {num, ""} -> {:ok, num}
      _ -> {:error, "value is not integer or out of range"}
    end
  end
end
