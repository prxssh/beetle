defmodule Beetle.Protocol.Encoder do
  @moduledoc """
  Implements an encoder for the Redis Serialization Protocol (RESP).

  RESP is a protocol that serializes different data types into a standardized
  format used by Redis for client-server communication. This module provides
  functions to convert Elixir data types into their corresponding RESP format.

  The encoder will raise an exception when attempting to encode unsupported
  data types.
  """
  @type encode_type_t :: nil | map() | list() | atom() | float() | integer() | binary()

  @resp_nil IO.iodata_to_binary("_\r\n")
  @resp_ok IO.iodata_to_binary("+OK\r\n")
  @resp_boolean_true IO.iodata_to_binary("#t\r\n")
  @resp_boolean_false IO.iodata_to_binary("#f\r\n")

  @spec encode(encode_type_t()) :: binary()
  def encode(nil), do: @resp_nil

  def encode(:ok), do: @resp_ok

  def encode(true), do: @resp_boolean_true

  def encode(false), do: @resp_boolean_false

  def encode({:error, reason}), do: IO.iodata_to_binary(["-", encode(reason), "\r\n"])

  def encode(data) when is_float(data),
    do: IO.iodata_to_binary([",", Float.to_string(data), "\r\n"])

  def encode(data) when is_integer(data),
    do: IO.iodata_to_binary([":", Integer.to_string(data), "\r\n"])

  def encode(data) when is_atom(data), do: encode(Atom.to_string(data))

  def encode(data) when is_binary(data) do
    len = byte_size(data)

    IO.iodata_to_binary(["$", Integer.to_string(len), "\r\n", data, "\r\n"])
  end

  def encode(data) when is_map(data) do
    items = Enum.flat_map(data, fn {k, v} -> [encode(k), encode(v)] end)

    IO.iodata_to_binary(["%", Integer.to_string(map_size(data)), "\r\n" | items])
  end

  def encode(data) when is_list(data) do
    items = Enum.map(data, &encode/1)

    IO.iodata_to_binary(["*", Integer.to_string(length(data)), "\r\n" | items])
  end

  def encode(data), do: raise("Unsupported data format: #{inspect(data)}")
end
