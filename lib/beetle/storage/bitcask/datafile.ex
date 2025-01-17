defmodule Beetle.Storage.Bitcask.Datafile do
  @moduledoc """
  Datafile is an append-only log file that holds the key-value pair along with
  some metadata. 

  A single Bitcask instance could have many datafiles, out of which only one
  will be active and opened for writing, while the other are considered
  immutable and are only used for reads. When the active datafile meets a size
  threshold, it is closed and a new active datafile is created.
  """
  require Logger
  alias Beetle.Storage.Bitcask.Datafile.Entry

  @type io_device_t :: :file.io_device()

  @type t :: %__MODULE__{
          writer: io_device_t(),
          reader: io_device_t(),
          offset: non_neg_integer()
        }
  defstruct [:writer, :reader, :offset]

  @default_read_buf_size 128 * 1024
  @default_write_buf_size 128 * 1024
  @default_flush_interval :timer.seconds(2)

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

    with {:ok, writer} <-
           :file.open(path, [
             :append,
             :raw,
             :binary,
             {:delayed_write, @default_write_buf_size, @default_flush_interval}
           ]),
         {:ok, reader} <-
           :file.open(path, [:read, :raw, :binary, {:read_ahead, @default_read_buf_size}]),
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

  @doc "Fetches the entry from the datafile stored at a particular position."
  @spec get(t(), non_neg_integer(), non_neg_integer()) ::
          {:ok, Datafile.Entry.t()} | {:error, any()}
  def get(datafile, pos, size) do
    datafile.reader
    |> Entry.get(pos, size)
    |> case do
      {:ok, entry} -> {:ok, entry}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Writes the key-value pair to the datafile and returns the datafile with the
  new offset.
  """
  @spec write(t(), Entry.key_t(), Entry.value_t(), non_neg_integer()) ::
          {:ok, {t(), non_neg_integer()}} | {:error, any()}
  def write(datafile, key, value, expiration) do
    entry = Entry.new(key, value, expiration)
    size = byte_size(entry)
    position = datafile.offset

    datafile.writer
    |> :file.write(entry)
    |> case do
      :ok ->
        {:ok, {%{datafile | offset: position + size}, size}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc "Dumps all entries in the datafile"
  @spec dump_all_entries(t()) :: {:ok, [{non_neg_integer(), Entry.t()}]} | {:error, any()}
  def dump_all_entries(datafile), do: Entry.dump_all(datafile.reader, 0, datafile.offset)

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
  import Beetle.Utils

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

  @header_size 16
  @tombstone_value <<0>>

  defstruct [:crc, :expiration, :key_size, :value_size, :key, :value]

  def deleted_sentinel, do: @tombstone_value

  @spec new(key_t(), value_t(), non_neg_integer()) :: binary()
  def new(key, value, expiration) do
    key_size = byte_size(key)
    serialized_value = serialize(value)
    value_size = byte_size(serialized_value)

    entry = [<<expiration::64, key_size::32, value_size::32>>, key, serialized_value]
    checksum = :erlang.crc32(entry)
    binary = :erlang.iolist_to_binary(entry)

    <<checksum::32, binary::binary>>
  end

  @spec get(:file.io_device(), non_neg_integer(), non_neg_integer()) ::
          {:ok, t()} | {:error, :expired | :deleted | any()}
  def get(io_device, pos, size) do
    with {:ok, binary} <- :file.pread(io_device, pos, size),
         {:ok, entry} <- decode_entry(binary),
         false <- expired?(entry.expiration),
         false <- deleted?(entry.value) do
      {:ok, entry}
    else
      true -> {:ok, nil}
      :eof -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec dump_all(:file.io_device(), non_neg_integer(), non_neg_integer(), list()) ::
          {:ok, [%{pos: non_neg_integer(), size: non_neg_integer(), entry: t()}]}
          | {:error, any()}
  def dump_all(io_device, current_offset, max_offset, acc \\ [])

  def dump_all(_, current_offset, max_offset, acc) when current_offset >= max_offset,
    do: {:ok, acc}

  def dump_all(io_device, current_offset, max_offset, acc) do
    io_device
    |> read(current_offset)
    |> case do
      {:ok, entry} ->
        next_offset = current_offset + @header_size + entry.key_size + entry.value_size

        new_acc =
          if expired?(entry) or deleted?(entry),
            do: acc,
            else: [%{pos: current_offset, size: next_offset, entry: entry} | acc]

        dump_all(io_device, next_offset, max_offset, new_acc)

      error ->
        error
    end
  end

  # === Private

  @spec read(:file.io_device(), non_neg_integer()) :: {:ok, t()} | {:error, any()}
  defp read(io_device, pos) do
    with {:ok, <<_::32, _::64, key_size::32, value_size::32>>} <-
           :file.pread(io_device, pos, @header_size),
         total_size <- @header_size + key_size + value_size,
         {:ok, binary} <- :file.pread(io_device, pos, total_size) do
      decode_entry(binary)
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @spec decode_entry(binary()) ::
          {:ok, t()} | {:error, :entry_invalid_checksum | :entry_invalid_format}
  defp decode_entry(<<crc::32, expiration::64, key_size::32, value_size::32, rest::binary>>) do
    with <<key::binary-size(key_size), value::binary-size(value_size)>> <- rest,
         entry_binary =
           <<expiration::64, key_size::32, value_size::32, key::binary, value::binary>>,
         true <- :erlang.crc32(entry_binary) == crc,
         {:ok, value} <- deserialize(value) do
      {:ok,
       %__MODULE__{
         crc: crc,
         key: key,
         value: value,
         key_size: key_size,
         value_size: value_size,
         expiration: expiration
       }}
    else
      false -> {:error, :entry_invalid_checksum}
      _ -> {:error, :entry_invalid_format}
    end
  end

  defp expired?(0), do: false
  defp expired?(expiration), do: System.system_time(:millisecond) >= expiration

  defp deleted?(@tombstone_value), do: true

  defp deleted?(_), do: false
end
