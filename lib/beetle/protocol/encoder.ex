defmodule Beetle.Protocol.Encoder do
  @moduledoc """
  Implements an encoder for the Redis Serialization Protocol (RESP).

  RESP is a protocol that serializes different data types into a standardized
  format used by Redis for client-server communication. This module provides
  functions to convert Elixir data types into their corresponding RESP format.

  ## Supported data types

  * `nil` - Encoded as Null type "_\r\n"
  * `:ok` - Encoded as Simple String "+OK\r\n"
  * `true` - Encoded as Boolean "#t\r\n"
  * `false` - Encoded as Boolean "#f\r\n"
  * `{:error, reason}` - Encoded as Error "-reason\r\n"
  * `atom` - Converted to string and encoded as Bulk String
  * `binary` - Encoded as Bulk String "$length\r\n[data]\r\n"
  * `integer` - Encoded as Integer ":value\r\n"
  * `float` - Encoded as Double/Float ",value\r\n" 
  * `map` - Encoded as Map "%size\r\n[key-value pairs]"
  * `list` - Encoded as Array "*length\r\n[elements]"

  The encoder will raise an exception when attempting to encode unsupported
  data types.
  """
  @resp_ok "+OK\r\n"
  @resp_nil "_\r\n"
  @resp_boolean_true "#t\r\n"
  @resp_boolean_false "#f\r\n"

  def encode(nil), do: @resp_nil
  def encode(:ok), do: @resp_ok
  def encode(true), do: @resp_boolean_true
  def encode(false), do: @resp_boolean_false

  def encode({:error, reason}), do: "-#{reason}\r\n"

  def encode(data) when is_float(data), do: ",#{data}\r\n"
  def encode(data) when is_integer(data), do: ":#{data}\r\n"
  def encode(data) when is_atom(data), do: encode(Atom.to_string(data))
  def encode(data) when is_binary(data), do: "$#{String.length(data)}\r\n#{data}\r\n"

  def encode(data) when is_map(data) do
    resp_encoded_map =
      data
      |> Enum.map_join(fn {k, v} ->
        encoded_key = encode(k)
        encoded_val = encode(v)
        encoded_key <> encoded_val
      end)

    "%#{map_size(data)}\r\n#{resp_encoded_map}"
  end

  def encode(data) when is_list(data) do
    resp_encoded_list = Enum.map_join(data, &encode/1)
    "*#{length(data)}\r\n#{resp_encoded_list}"
  end

  def encode(data), do: raise("Unsupported data format: #{inspect(data)}")
end
