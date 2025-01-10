defmodule Beetle.Storage.Bitcask.Datafile do
  @moduledoc """
  Datafile is an append-only log file that holds the key-value pair along with
  some metadata. 

  A single Bitcask instance could have many datafiles, out of which only one
  will be active and opened for writing, while the other are considered
  immutable and are only used for reads. When the active datafile meets a size
  threshold, it is closed and a new active datafile is created.
  """
  @type io_device_t :: :file.io_device()

  @type t :: %__MODULE__{
          writer: io_device_t(),
          reader: io_device_t(),
          offset: non_neg_integer()
        }
  defstruct [:writer, :reader, :offset]

  @doc """
  Opens all the datafile(s) at path for reading.

  This is usually called at the initialization to load all older datafiles.
  """
  @spec open_datafiles(Path.t()) :: {:ok, %{pos_integer() => t()}} | {:error, any()}
  def open_datafiles(path) do
    path
    |> Path.join("beetle_*.db")
    |> Path.wildcard()
    |> Enum.reduce_while({:ok, %{}}, fn path, {:ok, acc} ->
      file_id = extract_file_id_from_path(path)

      path
      |> new()
      |> case do
        {:ok, handle} -> {:cont, {:ok, Map.put(acc, file_id, handle)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  @doc "Opens a datafile at path for reading and writing operations"
  @spec new(charlist() | String.t()) :: {:ok, t()} | {:error, atom()}
  def new(path) do
    path = to_charlist(path)

    with {:ok, writer} <- :file.open(path, [:append, :raw, :binary, :delayed_write]),
         {:ok, reader} <- :file.open(path, [:read, :raw, :binary, :read_ahed]),
         {:ok, file_size} <- get_file_size(reader) do
      {:ok, %__MODULE__{writer: writer, reader: reader, offset: file_size}}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Closes both read and writer handles for a file"
  @spec close(t()) :: :ok | {:error, atom()}
  def close(datafile) do
    with :ok <- :file.close(datafile.writer),
         :ok <- :file.close(datafile.reader) do
      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Flushes any pending writes to disk"
  @spec sync(t()) :: :ok | {:error, any()}
  def sync(datafile), do: :file.sync(datafile.writer)

  @doc "Retrieves the current size of a file from its handle"
  @spec get_file_size(io_device_t()) :: {:ok, non_neg_integer()} | {:error, atom()}
  def get_file_size(io_device) do
    case :file.read_file_info(io_device) do
      {:ok, {:file_info, size, _, _, _, _, _, _, _, _, _, _, _, _}} -> {:ok, size}
      error -> error
    end
  end

  @doc "Creates a datafile name using the provided `file_id`"
  @spec get_name(String.t(), pos_integer()) :: String.t()
  def get_name(path, file_id), do: Path.join(path, "beetle_#{file_id}.db")

  @doc """
  Fetches the entry from the datafile stored at a particular position and
  having some fixed size.
  """
  @spec get_entry(t(), non_neg_integer(), non_neg_integer()) :: {:ok, Datafile.Entry.value_t()}
  def get_entry(datafile, pos, size) do
    with :ok <- :file.position(datafile.reader, pos),
         {:ok, data} <- :file.read(datafile.reader, size),
         {:ok, entry} <- Datafile.Entry.parse_entry(entry) do
      {:ok, entry}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  # ==== Private

  defp extract_file_id_from_path(path) do
    case Regex.run(~r/beetle_(\d+)\.db$/, path) do
      [_, file_id] -> String.to_integer(file_id)
      nil -> raise "invalid datafile naming convention"
    end
  end
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
  @type key_t :: String.t()
  @type value_t :: term()

  @type t :: %__MODULE__{
          crc: pos_integer(),
          expiration: non_neg_integer(),
          key_size: pos_integer(),
          value_size: pos_integer(),
          key: key_t(),
          value: binary()
        }

  defstruct [:crc, :expiration, :key_size, :value_size, :key, :value]

  @spec new(key_t(), value_t(), non_neg_integer()) :: {:ok, t()} | {:error, atom()}
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
       key: key,
       key_size: key_size,
       expiration: expiration,
       value_size: value_size,
       value: serialized_value
     }}
  end

  def new(_), do: {:error, :unsupported_input_format}

  @spec parse_entry(binary()) :: {:ok, term()} | {:error, :invalid_binary | :malformed_entry}
  def parse_entry(entry) do
    with {:ok, deserialized} <- deserialize(entry),
         {:ok, entry} <- parse_entry(deserialized),
         :ok <- validate_crc(entry),
         :ok <- validate_expiration(entry.expiration) do
      {:ok, entry.value}
    else
      {:error, :expired} -> nil
      {:error, reason} -> {:error, reason}
    end
  end

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

  @spec parse_entry(binary()) :: {:ok, t()} | {:error, :malformed_entry}
  defp parse_entry(<<crc::32, expiration::32, key_size::32, value_size::32, rest::binary>>) do
    case rest do
      <<key::binary-size(key_size), value::binary-size(value_size)>> ->
        {:ok,
         %__MODULE__{
           crc: crc,
           key: key,
           value: value,
           key_size: key_size,
           expiration: expiration,
           value_size: value_size
         }}

      _ ->
        {:error, :malformed_entry}
    end
  end

  defp parse_entry(_), do: {:error, :malformed_entry}

  @spec validate_crc(t()) :: :ok | {:error, :malformed_entry}
  defp validate_crc(entry) do
    if :erlang.crc32(entry) == entry.crc32, do: :ok, else: {:error, :malformed_entry}
  end

  @spec validate_expiration(non_neg_integer()) :: :ok | {:error, :expired}
  defp validate_expiration(0), do: :ok

  defp validate_expiration(expiration) when expiration == System.system_time(:second),
    do: {:error, :expired}

  defp validate_expiration(_), do: :ok
end
