defmodule Beetle.Parser.Core do
  @moduledoc """
  Core logic for the RESP Parser
  """

  alias Beetle.Parser.{
    Decoder,
    Encoder
  }

  @spec decode(binary()) :: {:ok, [any()]} | {:error, String.t() | Atom.t()}
  def decode(data), do: Decoder.decode(data)

  @spec encode(any()) :: String.t()
  def encode(data), do: Encoder.encode(data)
end
