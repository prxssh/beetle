defmodule Beetle.Protocol.Decoder do
  @moduledoc """
  To communicate with the Beetle server, clients use a protocol called Redis
  Serialization Protocol (RESP). This module implement a decoder for it.

  RESP can serialize different data types including integers, strings, and
  arrays. It also features an error-specific type. A client sends a request to
  the Beetle server as an array of bulk strings. The array's content are the
  command and its arguments that the server should execute. The server's reply
  type is command specific.

  RESP is binary-safe and users prefixed length to transfer bulk data so it
  doesn't require processing bulk data transferred from one process to another.

  The \r\n (CRLF) is the protocol's terminator, which always separates its
  parts. More documentation can be found
  [here](https://redis.io/docs/latest/develop/reference/protocol-spec/).
  """
  alias Beetle.Utils

  @spec decode(String.t(), [[String.t()]]) :: {:ok, [[String.t()]]} | {:error, String.t()}
  def decode(input, acc \\ [])

  def decode(<<>>, acc), do: {:ok, Enum.reverse(acc)}

  def decode(input, acc) when is_binary(input) do
    case do_decode(input) do
      {:ok, {decoded, rest}} -> decode(rest, [decoded | acc])
      error -> error
    end
  end

  def decode(_, _), do: {:error, "input must be a binary"}

  # === Simple String

  defp do_decode("+" <> rest), do: parse_line(rest)

  # === Simple Error

  defp do_decode("-" <> rest), do: parse_line(rest)

  # === Integer

  defp do_decode(":" <> rest) do
    with {:ok, {value_str, rest}} <- parse_line(rest),
         {:ok, value} <- Utils.parse_integer(value_str) do
      {:ok, {value, rest}}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  # === Bulk String

  defp do_decode("$" <> rest) do
    with {:ok, {length_str, rest}} <- parse_line(rest),
         {:ok, length} <- Utils.parse_integer(length_str) do
      extract_bulk_string(rest, length)
    else
      {:error, reason} -> {:error, reason}
    end
  end

  # === Array

  defp do_decode("*" <> rest) do
    with {:ok, {length_str, remaining}} <- parse_line(rest),
         {:ok, length} <- Utils.parse_integer(length_str) do
      decode_array_elements(remaining, length, [])
    else
      {:error, reason} -> {:error, reason}
    end
  end

  # === Nulls

  defp do_decode("_" <> rest) do
    case parse_line(rest) do
      {:ok, {_, remaining}} -> {:ok, {nil, remaining}}
      error -> error
    end
  end

  # === Boolean

  defp do_decode("#" <> rest) do
    case parse_line(rest) do
      {:ok, {boolean_str, remaining}} ->
        cond do
          boolean_str == "t" -> {:ok, {true, remaining}}
          boolean_str == "f" -> {:ok, {false, remaining}}
          true -> {:error, "invalid type for boolean conversion"}
        end
    end
  end

  # === Double

  defp do_decode("," <> rest) do
    with {:ok, {float_str, remaining}} <- parse_line(rest),
         {:ok, float} <- to_float(float_str) do
      {:ok, {float, remaining}}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  # === Big Number

  # Delegate decoding to integer
  defp do_decode("(" <> rest), do: do_decode(":" <> rest)

  # === Bulk Error

  # Delegate decoding to bulk string
  defp do_decode("!" <> rest), do: do_decode("$" <> rest)

  # === Map

  defp do_decode("%" <> rest) do
    # Get the count of entries, which can be decoded as an integer.
    with {:ok, {entries, remaining}} <- do_decode(":" <> rest),
         {:ok, {decoded_map, remaining}} <- decode_map_entries(remaining, entries, %{}) do
      {:ok, {decoded_map, remaining}}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  # === Set

  defp do_decode("~" <> rest) do
    # Set is decoded just like array except for the initial identifier.
    case do_decode("*" <> rest) do
      {:ok, {elements, remaining}} -> {:ok, {MapSet.new(elements), remaining}}
      {:error, reason} -> {:error, reason}
    end
  end

  # === Invalid Type

  defp do_decode(data), do: {:error, "invalid resp type '#{String.at(data, 0)}'"}

  # === Private Helpers

  @spec parse_line(String.t()) :: {:ok, {String.t(), String.t()}} | {:error, String.t()}
  defp parse_line(input) do
    case String.split(input, "\r\n", parts: 2) do
      [line, rest] -> {:ok, {line, rest}}
      _ -> {:error, "malformed line: missing CRLF"}
    end
  end

  @spec decode_array_elements(String.t(), non_neg_integer(), list()) ::
          {:ok, {list() | nil, String.t()}} | {:error, String.t()}
  defp decode_array_elements(input, -1, _), do: {:ok, {nil, input}}

  defp decode_array_elements(_, length, _) when length < -1,
    do: {:error, "invalid length of array '#{length}'"}

  defp decode_array_elements(input, 0, acc), do: {:ok, {Enum.reverse(acc), input}}

  defp decode_array_elements(input, count, acc) do
    case do_decode(input) do
      {:error, reason} -> {:error, reason}
      {:ok, {value, rest}} -> decode_array_elements(rest, count - 1, [value | acc])
    end
  end

  @spec decode_map_entries(String.t(), non_neg_integer(), map()) ::
          {:ok, {map(), String.t()}} | {:error, String.t()}
  defp decode_map_entries(input, 0, acc), do: {:ok, {acc, input}}

  defp decode_map_entries(input, count, acc) do
    with {:ok, {key, rest}} <- do_decode(input),
         {:ok, {value, rest}} <- do_decode(rest) do
      decode_map_entries(rest, count - 1, Map.put(acc, key, value))
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @spec to_float(String.t()) ::
          {:ok, float() | :infinity | :negative_infinity | :nan} | {:error, String.t()}
  defp to_float("inf"), do: {:ok, :infinity}

  defp to_float("-inf"), do: {:ok, :negative_infinity}

  defp to_float("nan"), do: {:ok, :nan}

  defp to_float(str) do
    case Float.parse(str) do
      {value, ""} -> {:ok, value}
      _ -> {:error, "invalid float string given for conversion"}
    end
  end

  @spec extract_bulk_string(String.t(), integer()) ::
          {:ok, {nil | String.t(), String.t()}} | {:error, String.t()}
  defp extract_bulk_string(data, -1), do: {:ok, {nil, data}}

  defp extract_bulk_string(_data, length) when length < -1,
    do: {:error, "invalid bulk string length '#{length}'"}

  defp extract_bulk_string(data, length) do
    case data do
      <<value::binary-size(length), "\r\n", rest::binary>> -> {:ok, {value, rest}}
      _ -> {:error, "insufficient data for bulk string"}
    end
  end
end
