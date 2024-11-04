defmodule Beetle.Parser.Decoder do
  @moduledoc """
  Decoder for the Redis Serialization Protocol (RESP).
  """

  @spec decode(binary()) :: {:ok, [[String.t()]]} | {:error, String.t() | Atom.t()}
  def decode(data) when is_binary(data), do: decode_recursive(data, [])

  @spec decode_recursive(binary(), [any()]) :: {:ok, [any()]} | {:error, String.t() | Atom.t()}
  defp decode_recursive(<<>>, acc), do: {:ok, Enum.reverse(acc)}

  defp decode_recursive(data, acc) do
    data
    |> do_decode()
    |> case do
      {:error, reason} -> {:error, reason}
      {:ok, decoded, rest} -> decode_recursive(rest, [decoded | acc])
    end
  end

  @spec do_decode(String.t(), list()) ::
          {:ok, [any()] | String.t()} | {:error, Atom.t() | String.t()}
  defp do_decode(data, acc \\ [])

  defp do_decode("", acc), do: {:ok, acc, ""}

  defp do_decode("*" <> rest, acc), do: decode_array(rest, acc)
  defp do_decode(":" <> rest, acc), do: decode_integer(rest, acc)
  defp do_decode("$" <> rest, acc), do: decode_bulk_string(rest, acc)
  defp do_decode("-" <> rest, acc), do: decode_simple_error(rest, acc)
  defp do_decode("+" <> rest, acc), do: decode_simple_string(rest, acc)
  defp do_decode(_, _), do: {:error, "invalid resp type"}

  # === Array

  defp decode_array(data, acc) do
    with {:ok, len, rest} <- decode_integer(data, acc),
         {:ok, elements, rem} <- decode_array_elements(rest, len, []) do
      {:ok, elements, rem}
    else
      error -> error
    end
  end

  defp decode_array_elements(_, len, _) when len < 0, do: {:error, "invalid array length"}
  defp decode_array_elements(data, 0, acc), do: {:ok, Enum.reverse(acc), data}

  defp decode_array_elements(data, len, acc) when len > 0 do
    case do_decode(data) do
      {:error, reason} -> {:error, reason}
      {:ok, element, rest} -> decode_array_elements(rest, len - 1, [element | acc])
    end
  end

  # === Integer

  defp decode_integer(data, acc) do
    data
    |> decode_simple_string(acc)
    |> case do
      {:error, reason} -> {:error, reason}
      {:ok, string, rest} -> {:ok, String.to_integer(string), rest}
    end
  end

  # === Bulk String

  defp decode_bulk_string(data, acc) do
    with {:ok, len, rest} <- decode_integer(data, acc),
         {:ok, string, rest} <- extract_bulk_string(rest, len) do
      {:ok, string, rest}
    else
      error -> error
    end
  end

  defp extract_bulk_string(_, len) when len < 0, do: {:error, "invalid length"}
  defp extract_bulk_string(_, len) when len == 0, do: {:ok, ""}

  defp extract_bulk_string(data, len) do
    case data do
      <<string::binary-size(len), "\r\n", rest::binary>> -> {:ok, string, rest}
      _ -> {:error, :incomplete}
    end
  end

  # === Simple Error

  defp decode_simple_error(data, acc), do: decode_simple_string(data, acc)

  # === Simple String

  defp decode_simple_string("", _), do: {:error, :incomplete}

  defp decode_simple_string("\r\n" <> rest, acc),
    do: {:ok, List.to_string(Enum.reverse(acc)), rest}

  defp decode_simple_string(<<char::utf8, rest::binary>>, acc),
    do: decode_simple_string(rest, [char | acc])
end
