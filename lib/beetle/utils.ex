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

  @spec serialize(term()) :: binary()
  def serialize(term), do: :erlang.term_to_binary(term, [:deterministic, :compressed])

  @spec deserialize(binary()) :: {:ok, term()} | {:error, String.t()}
  def deserialize(binary) when is_binary(binary), do: {:ok, :erlang.binary_to_term(binary)}
  def deserialize(_), do: {:error, "MALFORMED_ERLANG_BINARY"}

  @spec error_command_arguments(String.t()) :: {:error, String.t()}
  def error_command_arguments(command),
    do: {:error, "ERR invalid number of arguments for '#{command}' command"}
end
