defmodule Beetle.Storage.Bitcask.Datafile do
  @moduledoc """
  Datafile is an append-only log file that holds the key-value pair along with
  some metadata. 

  A single Bitcask instance could have many datafiles, out of which only one
  will be active and opened for writing, while the other are considered
  immutable and are only used for reads. When the active datafile meets a size
  threshold, it is closed and a new active datafile is created.
  """
  alias Beetle.Storage.Bitcask.Datafile.Entry

  @typedoc """
  Represents a datafile, which is a segment of an append-only log contaiing
  key-value pairs. 

  A datafile has both read and write handles and tracks its current write
  offset. Only one datafile is active for writing at a time.
  """
  @type t :: %__MODULE__{
          writer: :file.io_device(),
          reader: :file.io_device(),
          offset: non_neg_integer()
        }

  @typedoc """
  Maps datafile IDs to their corresponding datafile structs. Used to track and
  manage all historical datafiles in the system.
  """
  @type file_id_t :: non_neg_integer()
  @type map_t :: %{file_id_t() => t()}

  @default_read_buf_size 128 * 1024
  @default_write_buf_size 128 * 1024
  @default_flush_interval :timer.seconds(2)

  defstruct [:writer, :reader, :offset]

  @doc """
  Opens all the datafile(s) at path for reading.

  This is usually called at the initialization to load all older datafiles.
  """
  @spec open(Path.t()) :: {:ok, map_t()} | {:error, any()}
  def open(path) do
    path
    |> Path.join("beetle_*.db")
    |> Path.wildcard()
    |> Enum.reduce_while({:ok, %{}}, fn path, {:ok, acc} ->
      file_id = parse_datafile_id(path)

      path
      |> new()
      |> case do
        {:ok, handle} -> {:cont, {:ok, Map.put(acc, file_id, handle)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  @doc """
  Opens a datafile at the given `path` with both read and write access.

  The file is opened in raw mode with buffered I/O for performance.
  """
  @spec new(charlist() | String.t()) :: {:ok, t()} | {:error, atom()}
  def new(path) do
    with path <- to_charlist(path),
         {:ok, writer} <-
           :file.open(path, [
             :append,
             :raw,
             :binary,
             {:delayed_write, @default_write_buf_size, @default_flush_interval}
           ]),
         {:ok, reader} <-
           :file.open(path, [:read, :raw, :binary, {:read_ahead, @default_read_buf_size}]),
         {:ok, file_size} <- file_bytes(reader) do
      {:ok, %__MODULE__{writer: writer, reader: reader, offset: file_size}}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Closes both read and writer handles for a file"
  @spec close(t()) :: :ok | {:error, atom()}
  def close(datafile) do
    with :ok <- sync(datafile),
         :ok <- :file.close(datafile.writer),
         :ok <- :file.close(datafile.reader) do
      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Flushes any pending writes to disk"
  @spec sync(t()) :: :ok | {:error, any()}
  def sync(datafile), do: :file.sync(datafile.writer)

  @doc "Constructs the full path for a datafile with the given ID"
  @spec build_path(String.t(), pos_integer()) :: String.t()
  def build_path(path, file_id), do: Path.join(path, "beetle_#{file_id}.db")

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
  Writes an entry to the datafile and returns the updated datafile with its new
  write position.
  """
  @spec write(t(), Entry.key_t(), Entry.value_t(), non_neg_integer()) ::
          {:ok, t(), non_neg_integer()} | {:error, any()}
  def write(datafile, key, value, expiration) do
    entry = Entry.new(key, value, expiration)
    size = byte_size(entry)

    datafile.writer
    |> :file.write(entry)
    |> case do
      :ok ->
        {:ok, %{datafile | offset: datafile.offset + size}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Scans and streams valid entries from a datafile.

  Uses `Stream.unfold/2` to lazily read entries from the datafile starting at
  offset 0 up to :eof. Each valid entry is returned with its position and size
  metadata. Deleted and expired entries are reject of the resulting stream.

  Returns an enumerable of type `Entry.metadata_t`.
  """
  @spec scan_valid_entries(t()) :: Enumerable.t()
  def scan_valid_entries(datafile) do
    Stream.unfold(0, fn
      current_offset when current_offset >= max_offset ->
        nil

      current_offset ->
        datafile.reader
        |> case read(current_offset) do
          :eof ->
            nil

          {:error, _reason} ->
            nil

          {:ok, entry_with_metadata} ->
            {entry_with_metadata, current_offset + entry_with_metadata.size}
        end
    end)
    |> Stream.reject(fn %{entry: entry} ->
      Entry.expired?(entry.expiration) or Entry.deleted?(entry.value)
    end)
  end

  # ==== Private

  # Extracts the numeric ID from a datafile path (e.g. "beetle_123.db" -> 123).
  #
  # Expects the filename to match the pattern "beetle_<number>.db". Raises if the
  # path doesn't follow the naming convention.
  defp parse_datafile_id(path) do
    case Regex.run(~r/beetle_(\d+)\.db$/, path) do
      [_, file_id] -> String.to_integer(file_id)
      nil -> raise "invalid datafile naming convention"
    end
  end

  # Gets the current file size from an open file handle
  @spec file_bytes(:file.io_device()) :: {:ok, non_neg_integer()} | {:error, atom()}
  def file_bytes(io_device) do
    case :file.read_file_info(io_device) do
      {:ok, {:file_info, size, _, _, _, _, _, _, _, _, _, _, _, _}} -> {:ok, size}
      error -> error
    end
  end
end

defmodule Beetle.Storage.Bitcask.Datafile.Entry do
  @moduledoc """
  Represents an entry in a Bitcask Datafile.

  Each entry in the datafile has the following format: 

    ----------------------------------------------------------
    | crc | expiration | key_size | value_size | key | value |
    ----------------------------------------------------------

  - `crc`: CRC32 hash of the entry (expiration + key size + value size + key + value)
  - `expiration`: unsigned integer representing the TTL for the key (0 for no expiration)
  - `key_size`: size of key in bytes
  - `value_size`: size of value in bytes
  - `value`: value serialized using `:erlang.term_to_binary/1`

  All integers are stored in big-endian format. The CRC is calculated over all
  the fields that follow it in the entry.

  Values can be any Erlang/Elixir term (lists, maps, sets, tuples, etc) as they
  are automatically serialized before storage and deserialized upon retrieval.
  """
  import Beetle.Utils

  @typedoc "Type of the key. Beetle only allows string keys"
  @type key_t :: String.t()

  @typedoc """
  Value can be any elixir term, though the interface exposed to the client only
  allows for these value types -- string, hash, list, bitmap, bloom filter, and
  set.
  """
  @type value_t :: term()

  @typedoc "Represents an entry stored in the datafile"
  @type t :: %__MODULE__{
          crc: pos_integer(),
          expiration: non_neg_integer(),
          key_size: pos_integer(),
          value_size: pos_integer(),
          key: key_t(),
          value: binary()
        }

  @typedoc """
  Represents an entry's metadata and location in the datafile. It contains:
  - `entry`: decoded entry struct with the key-value data
  - `size`: total size of the entry in bytes
  - `position`: byte offset where entry begins in the datafile

  Used for tracking storage details for entries during operations like
  compaction or building keydir.
  """
  @type metadata_t :: %{
          entry: t(),
          size: non_neg_integer(),
          position: non_neg_integer()
        }

  @header_size 16
  @tombstone_value <<0>>

  defstruct [:crc, :expiration, :key_size, :value_size, :key, :value]

  @doc "Creates a new serialized entry for storage in the datafile"
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

  @doc "Reads and decodes an entry from the datafile at the specified position"
  @spec get(:file.io_device(), non_neg_integer(), non_neg_integer()) ::
          {:ok, t()} | {:error, term()}
  def get(io_device, pos, size) do
    with {:ok, binary} <- :file.pread(io_device, pos, size),
         {:ok, entry} <- decode_entry(binary),
         false <- expired?(entry.expiration),
         false <- deleted?(entry.value) do
      {:ok, entry}
    else
      true -> {:ok, nil}
      :eof -> {:error, :eof_reached}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Reads a raw entry from the datafile from the specified the position.

  This function does two seek operation to fully read the entry. Almost always
  reach out for `get/2` to read the entries. This function should be used only
  when you don't have information about the entry size i.e. when builiding
  keydir from the datafiles.
  """
  @spec read_raw(:file.io_device(), non_neg_integer()) ::
          {:ok, metadata_t()} | :eof | {:error, term()}
  def read_raw(io_device, pos) do
    with {:ok, <<_::32, _::64, key_size::32, value_size::32>>} <-
           :file.pread(io_device, pos, @header_size),
         total_size <- @header_size + key_size + value_size,
         {:ok, binary} <- :file.pread(io_device, pos, total_size),
         {:ok, entry} <- decode_entry(binary) do
      {:ok, %{entry: entry, position: position, size: pos + total_size}}
    else
      :eof -> :eof
      {:error, reason} -> {:error, reason}
    end
  end

  def expired?(%__MODULE__{expiration: 0}), do: false

  def expired?(%__MODULE__{expiration: expiration}),
    do: System.system_time(:millisecond) >= expiration

  def deleted?(%__MODULE__{value: @tombstone_value}), do: true
  def deleted?(_), do: false

  # === Private

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
end
