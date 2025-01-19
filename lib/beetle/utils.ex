defmodule Beetle.Utils do
  @moduledoc false

  @spec serialize(term()) :: binary()
  def serialize(term), do: :erlang.term_to_binary(term, [:deterministic, :compressed])

  @spec deserialize(binary()) :: {:ok, term()} | {:error, :malformed_binary_type}
  def deserialize(binary) when is_binary(binary), do: {:ok, :erlang.binary_to_term(binary)}
  def deserialize(_), do: {:error, :malformed_binary_type}

  def parse_integer(str) do
    case Integer.parse(str) do
      {num, ""} -> {:ok, num}
      _ -> {:error, "value is not an integer or out of range"}
    end
  end
end
