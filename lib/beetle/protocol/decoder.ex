defmodule Beetle.Protocol.Decoder do
  @moduledoc """
  Implements a decoder for the Redis Serialization Protocol (RESP) used by
  Beetle server.

  RESP is a binary-safe protocol that serializes different data types using a
  prefixed length approach. Each message is terminated with CRLF (`\\r\\n`).
  The protocol is designed for efficient communication between clients and the
  server.

  ## Supported Data Types

  * Simple String - Prefixed with `+` (e.g., `+OK\r\n`)
  * Simple Error - Prefixed with `-` (e.g., `-ERR unknown command\r\n`)
  * Integer - Prefixed with `:` (e.g., `:1000\r\n`)
  * Bulk String - Prefixed with `$` followed by string length (e.g., `$5\r\nhello\r\n`)
  * Array - Prefixed with `*` followed by array length (e.g., `*2\r\n$3\r\nGET\r\n$4\r\nkeys\r\n`)
  * Null - Represented as `_\r\n`
  * Boolean - Prefixed with `#` (`#t\r\n` for true, `#f\r\n` for false)
  * Double/Float - Prefixed with `,` (e.g., `,3.14159\r\n`)
  * Big Number - Prefixed with `(`
  * Bulk Error - Prefixed with `!`
  * Map - Prefixed with `%` followed by number of entries
  * Set - Prefixed with `~` followed by array format

  ## Usage

  The main function is `decode/1` (or `decode/2` with accumulator), which takes
  a binary RESP payload and returns either `{:ok, decoded_values}` or `{:error,
  reason}`.

  ```elixir
  # Decode a simple RESP string
  {:ok, values} = Beetle.Protocol.Decoder.decode("+OK\r\n")
  ```

  # Decode a complex RESP array containing different data types
  {:ok, values} = Beetle.Protocol.Decoder.decode("*3\r\n:1\r\n$5\r\nhello\r\n#t\r\n")

  # Handle potential errors
  case Beetle.Protocol.Decoder.decode(input) do
    {:ok, decoded} -> # Process decoded data
    {:error, reason} -> # Handle error
  end
  ```

  The decoder uses a recursive approach to process nested data types such as
  arrays and maps. Special values like infinity and NaN are properly handled in
  floating-point numbers.

  For more details on the RESP specification, see the 
  [Redis protocol specification](https://redis.io/docs/latest/develop/reference/protocol-spec/).
  """
  alias Beetle.Utils

  @crlf "\r\n"
  @crlf_size 2

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

  defp do_decode(<<"+"::binary, rest::binary>>), do: parse_line(rest)

  # === Simple Error

  defp do_decode(<<"-"::binary, rest::binary>>), do: parse_line(rest)

  # === Integer

  defp do_decode(<<":"::binary, rest::binary>>) do
    with {:ok, {value_str, rest}} <- parse_line(rest),
         {:ok, value} <- Utils.parse_integer(value_str) do
      {:ok, {value, rest}}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  # === Bulk String

  defp do_decode(<<"$"::binary, rest::binary>>) do
    with {:ok, {length_str, rest}} <- parse_line(rest),
         {:ok, length} <- Utils.parse_integer(length_str) do
      extract_bulk_string(rest, length)
    else
      {:error, reason} -> {:error, reason}
    end
  end

  # === Array

  defp do_decode(<<"*"::binary, rest::binary>>) do
    with {:ok, {length_str, remaining}} <- parse_line(rest),
         {:ok, length} <- Utils.parse_integer(length_str) do
      decode_array_elements(remaining, length, [])
    else
      {:error, reason} -> {:error, reason}
    end
  end

  # === Nulls

  defp do_decode(<<"_"::binary, rest::binary>>) do
    case parse_line(rest) do
      {:ok, {_, remaining}} -> {:ok, {nil, remaining}}
      error -> error
    end
  end

  # === Boolean

  defp do_decode(<<"#t\r\n"::binary, rest::binary>>), do: {:ok, {true, rest}}
  defp do_decode(<<"#f\r\n"::binary, rest::binary>>), do: {:ok, {false, rest}}
  defp do_decode(_), do: {:error, "invalid type for boolean conversion"}

  # === Double

  defp do_decode(<<","::binary, rest::binary>>) do
    with {:ok, {float_str, remaining}} <- parse_line(rest),
         {:ok, float} <- to_float(float_str) do
      {:ok, {float, remaining}}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  # === Big Number

  # Delegate decoding to integer
  defp do_decode(<<"("::binary, rest::binary>>), do: do_decode(<<":"::binary, rest::binary>>)

  # === Bulk Error

  # Delegate decoding to bulk string
  defp do_decode(<<"!"::binary, rest::binary>>), do: do_decode(<<"$"::binary, rest::binary>>)

  # === Map

  defp do_decode(<<"%"::binary, rest::binary>>) do
    # Get the count of entries, which can be decoded as an integer.
    with {:ok, {entries, remaining}} <- do_decode(<<":"::binary, rest::binary>>),
         {:ok, {decoded_map, remaining}} <- decode_map_entries(remaining, entries, Map.new()) do
      {:ok, {decoded_map, remaining}}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  # === Set

  defp do_decode(<<"~"::binary, rest::binary>>) do
    # Set is decoded just like array except for the initial identifier.
    case do_decode(<<"*"::binary, rest::binary>>) do
      {:ok, {elements, remaining}} -> {:ok, {MapSet.new(elements), remaining}}
      {:error, reason} -> {:error, reason}
    end
  end

  # === Invalid Type

  defp do_decode(<<first::binary-size(1), _::binary>>),
    do: {:error, "invalid resp type '#{first}'"}

  # === Private Helpers

  @spec parse_line(binary()) :: {:ok, {String.t(), String.t()}} | {:error, String.t()}
  defp parse_line(input) do
    case :binary.split(input, @crlf) do
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
      {:ok, {value, rest}} -> decode_array_elements(rest, count - 1, [value | acc])
      {:error, reason} -> {:error, reason}
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
      error -> error
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
