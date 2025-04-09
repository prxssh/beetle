defmodule Beetle.Protocol.Decoder do
  @moduledoc """
  Implements a decoder for the Redis Serialization Protocol Specification.

  Redis Serialization Protocol Specification (RESP) is a binary-safe protocol
  that serializes different data types using a prefixed length approach. Each
  message is terminated with CRLF (`\r\n`). The protocol is designed for
  efficient communication between clients and the server.

  For more details on the RESP specification, see the 
  [Redis protocol specification](https://redis.io/docs/latest/develop/reference/protocol-spec/).
  """
  import Beetle.Utils

  @type double_type_t :: float() | :infinity | :negative_infinity | :nan

  @type decoded_type_t ::
          nil
          | map()
          | integer()
          | String.t()
          | MapSet.t()
          | double_type_t()
          | [String.t() | integer() | double_type_t()]

  @doc "Decodes a RESP encoded string"
  @spec decode(binary(), [decoded_type_t()]) :: {:ok, [decoded_type_t()]} | {:error, String.t()}
  def decode(input, acc \\ [])

  def decode(<<>>, acc), do: {:ok, Enum.reverse(acc)}

  def decode(input, acc) when is_binary(input) do
    case do_decode(input) do
      {:ok, {decoded, rest}} -> decode(rest, [decoded | acc])
      {:error, reason} -> {:error, reason}
    end
  end

  def decode(_, _), do: {:error, "input must be a binary"}

  ############### Private

  ##### Simple String
  defp do_decode(<<"+"::binary, rest::binary>>), do: parse_line(rest)

  ##### Simple Error
  defp do_decode(<<"-"::binary, rest::binary>>), do: parse_line(rest)

  ##### Simple Integer
  defp do_decode(<<":"::binary, rest::binary>>) do
    with {:ok, {value_str, rest}} <- parse_line(rest),
         {:ok, value} <- parse_integer(value_str) do
      {:ok, {value, rest}}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  ##### Double
  defp do_decode(<<","::binary, rest::binary>>) do
    with {:ok, {float_str, rest}} <- parse_line(rest),
         {:ok, float} <- to_float(float_str) do
      {:ok, {float, rest}}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  ##### Bulk String
  defp do_decode(<<"$"::binary, rest::binary>>) do
    with {:ok, {len_str, rest}} <- parse_line(rest),
         {:ok, len} <- parse_integer(len_str) do
      extract_bulk_string(rest, len)
    else
      {:error, reason} -> {:error, reason}
    end
  end

  ##### Array
  defp do_decode(<<"*"::binary, rest::binary>>) do
    with {:ok, {len_str, rest}} <- parse_line(rest),
         {:ok, len} <- parse_integer(len_str) do
      decode_array_elements(rest, len)
    else
      {:error, reason} -> {:error, reason}
    end
  end

  ##### Null
  defp do_decode(<<"_"::binary, rest::binary>>) do
    case parse_line(rest) do
      {:ok, {_, rest}} -> {:ok, {nil, rest}}
      {:error, reason} -> {:error, reason}
    end
  end

  ##### Boolean
  defp do_decode(<<"#t\r\n"::binary, rest::binary>>), do: {:ok, {true, rest}}
  defp do_decode(<<"#f\r\n"::binary, rest::binary>>), do: {:ok, {false, rest}}
  defp do_decode(<<"#"::binary, _::binary>>), do: {:error, "invalid boolean type"}

  ##### Big Number
  defp do_decode(<<"("::binary, rest::binary>>), do: do_decode(<<":"::binary, rest::binary>>)

  ##### Bulk Error
  defp do_decode(<<"!"::binary, rest::binary>>), do: do_decode(<<"$"::binary, rest::binary>>)

  ##### Map
  defp do_decode(<<"%"::binary, rest::binary>>) do
    case do_decode(<<":"::binary, rest::binary>>) do
      {:ok, {entries, rest}} -> decode_map_entries(rest, entries)
      {:error, reason} -> {:error, reason}
    end
  end

  ##### Set
  defp do_decode(<<"~"::binary, rest::binary>>) do
    # Set is decoded just like array except for the initial identifier.
    case do_decode(<<"*"::binary, rest::binary>>) do
      {:ok, {elements, rest}} -> {:ok, {MapSet.new(elements), rest}}
      {:error, reason} -> {:error, reason}
    end
  end

  ##### Invalid Type
  defp do_decode(input),
    do: {:error, "invalid input for resp decoder '#{input}'"}

  @spec parse_line(binary()) :: {:ok, {binary(), binary()}} | {:error, String.t()}
  defp parse_line(line) do
    case :binary.split(line, "\r\n") do
      [line, rest] -> {:ok, {line, rest}}
      _ -> {:error, "malformed line: missing CRLF"}
    end
  end

  @spec to_float(String.t()) :: {:ok, double_type_t()} | {:error, String.t()}
  defp to_float("inf"), do: {:ok, :infinity}

  defp to_float("-inf"), do: {:ok, :negative_infinity}

  defp to_float("nan"), do: {:ok, :nan}

  defp to_float(str) do
    case Float.parse(str) do
      {value, ""} -> {:ok, value}
      _ -> {:error, "invalid float string given for conversion"}
    end
  end

  @spec extract_bulk_string(binary(), integer()) ::
          {:ok, {nil | binary(), binary()}} | {:error, String.t()}
  defp extract_bulk_string(data, -1), do: {:ok, {nil, data}}

  defp extract_bulk_string(_, len) when len < -1,
    do: {:error, "invalid bulk string: length can't be #{len}"}

  defp extract_bulk_string(data, len) do
    case data do
      <<val::binary-size(len), "\r\n", rest::binary>> -> {:ok, {val, rest}}
      _ -> {:error, "malformed bulk string: insufficient data"}
    end
  end

  @spec decode_array_elements(binary(), integer(), [any()]) ::
          {:ok, {nil | [any()], binary()}} | {:error, String.t()}
  defp decode_array_elements(data, count, acc \\ [])

  defp decode_array_elements(data, -1, _), do: {:ok, {nil, data}}

  defp decode_array_elements(data, 0, acc), do: {:ok, {Enum.reverse(acc), data}}

  defp decode_array_elements(_, len, _) when len < -1,
    do: {:error, "array length can't be '#{len}'"}

  defp decode_array_elements(data, count, acc) do
    case do_decode(data) do
      {:ok, {val, rest}} -> decode_array_elements(rest, count - 1, [val | acc])
      {:error, reason} -> {:error, reason}
    end
  end

  @spec decode_map_entries(String.t(), integer(), map()) ::
          {:ok, {map(), String.t()}} | {:error, String.t()}
  defp decode_map_entries(data, count, acc \\ %{})

  defp decode_map_entries(data, 0, acc), do: {:ok, {acc, data}}

  defp decode_map_entries(_, count, _) when count < 0,
    do: {:error, "invalid map: elements can't be '#{count}'"}

  defp decode_map_entries(data, count, acc) do
    with {:ok, {key, rest}} <- do_decode(data),
         {:ok, {val, rest}} <- do_decode(rest) do
      decode_map_entries(rest, count - 1, Map.put(acc, key, val))
    else
      {:error, reason} -> {:error, reason}
    end
  end
end
