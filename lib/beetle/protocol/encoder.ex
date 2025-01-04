defmodule Beetle.Protocol.Encoder do
  @moduledoc """
  Implements an encoder for the Redis Serialization Protocol (RESP).

  RESP is a protocol that serializes different data types including strings,
  integers, arrays, errors, maps, sets, etc. This module provides encoding
  functionality for converting Elixir data types into RESP format.
  """
  def encode(nil), do: "_\r\n"

  def encode(true), do: "#t\r\n"

  def encode(false), do: "#f\r\n"

  def encode({:error, reason}), do: "-" <> "#{reason}" <> "\r\n"

  def encode(data) when is_atom(data), do: encode(Atom.to_string(data))

  def encode(data) when is_binary(data),
    do: "$" <> "#{String.length(data)}" <> "\r\n" <> data <> "\r\n"

  def encode(data) when is_integer(data), do: ":" <> "#{data}" <> "\r\n"

  def encode(data) when is_float(data), do: "," <> "#{data}" <> "\r\n"

  def encode(data) when is_map(data) do
    resp_encoded_map =
      data
      |> Enum.map(fn {k, v} ->
        encoded_key = encode(k)
        encoded_val = encode(v)
        encoded_key <> encoded_val
      end)
      |> Enum.join()

    "%" <> "#{map_size(data)}" <> "\r\n" <> resp_encoded_map
  end

  def encode(data) when is_list(data) do
    resp_encoded_list = data |> Enum.map(&encode/1) |> Enum.join()

    "*" <> "#{length(data)}" <> "\r\n" <> resp_encoded_list
  end

  def encode(data), do: raise("Unsupported data format: #{inspect(data)}")
end
