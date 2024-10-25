defmodule Beetle.Parser.Encoder do
  @moduledoc """
  Encodes the data to Redis Serialization Protocol (RESP).
  """

  def encode({:error, reason}), do: encode_error(reason)
  def encode(data) when is_list(data), do: encode_array(data)
  def encode(data) when is_binary(data), do: encode_string(data)
  def encode(data) when is_integer(data), do: encode_integer(data)

  defp encode_error(data), do: "-ERR " <> data <> "\r\n"

  defp encode_array(data) do
    len = length(data)

    encoded_elements =
      Enum.reduce(data, "", fn d, acc ->
        encoded = encode(d)
        acc <> encoded
      end)

    # elements are aleady terminated by CRLF, we don't need to add it here
    "*" <> "#{len}\r\n" <> encoded_elements
  end

  defp encode_string(data), do: "$" <> "#{String.length(data)}" <> "\r\n" <> data <> "\r\n"

  defp encode_integer(data), do: ":" <> "#{data}" <> "\r\n"
end
