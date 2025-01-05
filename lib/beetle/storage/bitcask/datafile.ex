defmodule Beetle.Storage.Bitcask.Datafile do
  @moduledoc """
  Datafile is an append-only log file that holds the key-value pair along with
  some metadata. 

  A single Bitcask instance could have many datafiles, out of which only one
  will be active and opened for writing, while the other are considered
  immutable and are only used for reads. When the active datafile meets a size
  threshold, it is closed and a new active datafile is created.
  """
end

defmodule Beetle.Storage.Bitcask.Datafile.Entry do
  @moduledoc """
  Represents an entry in a Bitcask Datafile.

  Each entry in the datafile has the following format: 

     --------------------------------------------------------------------
    | crc | expiration | key_size | value_size | key | serialized_value |
    --------------------------------------------------------------------

  - `crc`: CRC32 hash of the entry (expiration + key size + value size + key + value)
  - `expiration`: unsigned integer representing the TTL for the key (0 for no expiration)
  - `key_size`: size of key in bytes
  - `value_size`: size of value in bytes
  - `serialized_value`: value serialized using `:erlang.term_to_binary/1`

  All integers are stored in big-endian format. The CRC is calculated over all
  the fields that follow it in the entry.

  Values can be any Erlang/Elixir term (lists, maps, sets, tuples, etc) as they
  are automatically serialized before storage and deserialized upon retrieval.
  """
  @type t :: %__MODULE__{
          crc: pos_integer(),
          expiration: non_neg_integer(),
          key_size: pos_integer(),
          value_size: pos_integer(),
          key: String.t(),
          value: binary()
        }

  defstruct [:crc, :expiration, :key_size, :value_size, :key, :value]

  @spec new(String.t(), term(), non_neg_integer()) :: {:ok, t()} | {:error, atom()}
  def new(key, value, expiration \\ 0)
      when is_binary(key) and is_integer(expiration) and expiration >= 0 do
    serialized_value = serialize(value)
    key_size = byte_size(key)
    value_size = byte_size(serialized_value)

    entry = <<expiration::32, key_size::32, value_size::32>> <> key <> serialized_value
    crc = :erlang.crc32(entry)

    {:ok,
     %__MODULE__{
       crc: crc,
       expiration: expiration,
       key_size: key_size,
       value_size: value_size,
       key: key,
       value: serialized_value
     }}
  end

  def new(_), do: {:error, :unsupported_input_format}

  # === Private

  @spec serialize(term()) :: binary()
  defp serialize(value), do: :erlang.term_to_binary(value)

  @spec deserialize!(binary()) :: term()
  defp deserialize!(binary) do
    case deserialize(binary) do
      {:ok, value} -> {:ok, value}
      {:error, reason} -> raise reason
    end
  end

  @spec deserialize(binary()) :: {:ok, term()} | {:error, atom()}
  defp deserialize(binary) when is_binary(binary), do: {:ok, :erlang.binary_to_term(binary)}

  defp deserialize(_), do: {:error, :invalid_binary}
end
