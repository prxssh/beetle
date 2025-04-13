defmodule Beetle.Storage.Bitcask.Datafile do
  @moduledoc """
  Datafile is an append-only log file that holds the key-value pair along with
  some metadata. 

  A single Bitcask instance could have many datafiles, out of which only one
  will be active and opened for writing, while the other are considered
  immutable and are only used for reads. When the active datafile meets a size
  threshold, it is closed and a new active datafile is created.
  """
  alias Beetle.Storage.Bitcask
  alias Beetle.Storage.Bitcask.Datafile.Entry, as: DatafileEntry

  @typedoc """
  Represents a datafile, which is part of an append-only log containing
  key-value pairs. Each datafile includes:
    - `writer`: write handle
    - `reader`: read handle
    - `offset`: current write offset for the file
  """
  @type t :: %__MODULE__{
          writer: :file.io_device(),
          reader: :file.io_device(),
          offset: non_neg_integer()
        }

  @typedoc """
  Identifies a specific datafile. Typically corresponds to the integer ID in
  filenames like: "beetle_123.db".
  """
  @type file_id_t :: non_neg_integer()

  @typedoc """
  Maps a file_id to the actual `t()` struct. For example:
    %{
      1 => %Beetle.Storage.Bitcask.Datafile{...},
      2 => %Beetle.Storage.Bitcask.Datafile{...},
      ...
    }
  """
  @type map_t :: %{file_id_t() => t()}

  @default_read_buf_size 128 * 1024
  @default_write_buf_size 128 * 1024
  @default_flush_interval :timer.seconds(2)

  defstruct [:writer, :reader, :offset]

  @doc """
  Opens a datafile at `path` with both read and write access.

  The file is opened in raw mode with buffered I/O for performance.
  """
  @spec new(Path.t()) :: {:ok, t()} | {:error, String.t()}
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
         {:ok, file_size} <- file_size(reader) do
      {:ok, %__MODULE__{writer: writer, reader: reader, offset: file_size}}
    else
      {:error, reason} -> {:error, inspect(reason)}
    end
  end

  @doc """
  Opens all the datafile(s) at `path` for reading. 

  This is particularly useful at the database bootup when we need to load all
  other datafiles.
  """
  @spec open(Path.t()) :: {:ok, map_t()} | {:error, String.t()}
  def open(path) do
    path
    |> Path.join("beetle_*.db")
    |> Path.wildcard()
    |> Enum.reduce_while({:ok, %{}}, fn path, {:ok, acc} ->
      file_id = parse_datafile_id(path)

      case new(path) do
        {:ok, handle} -> {:cont, {:ok, Map.put(acc, file_id, handle)}}
        error -> {:halt, error}
      end
    end)
  end

  @doc "Closes both read and write handles for the datafile"
  @spec close(t()) :: :ok | {:error, term()}
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
  @spec sync(t()) :: :ok | {:error, term()}
  def sync(%__MODULE__{writer: writer}), do: :file.sync(writer)

  @doc "Constructs a full path for a datafile with the given `file_id`."
  @spec path(String.t() | charlist(), non_neg_integer()) :: binary()
  def path(path, file_id), do: Path.join(path, "beetle_#{file_id}.db")

  @doc "Get an entry from the datafile stored at `pos` having `size`."
  @spec get(t(), non_neg_integer(), non_neg_integer()) ::
          {:ok, DatafileEntry.t() | nil} | {:error, String.t()}
  def get(%__MODULE__{reader: reader}, pos, size) do
    case DatafileEntry.get(reader, pos, size) do
      {:ok, entry} -> {:ok, entry}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Writes a new entry to the datafile with the key, value, and expiration.

  Returns the datafile with updated write offset upon success.
  """
  @spec write(t(), DatafileEntry.key_t(), DatafileEntry.value_t(), Bitcask.put_opts_t()) ::
          {:ok, t()} | {:error, String.t()}
  def write(datafile, key, value, opts) do
    entry = DatafileEntry.new(key, value, opts[:expiration])
    size = byte_size(entry)

    case :file.write(datafile.writer, entry) do
      :ok -> {:ok, %__MODULE__{datafile | offset: datafile.offset + size}}
      {:error, reason} -> {:error, inspect(reason)}
    end
  end

  @doc """
  Lazily scans the datafile and streams valid entries.

  Entries are read from offset 0 up to the current write offset. Any entries
  that are deleted or expired are rejected.
  """
  @spec scan_valid_entries(t()) :: Enumerable.t()
  def scan_valid_entries(datafile) do
    Stream.unfold(0, fn
      current_offset when current_offset >= datafile.offset ->
        nil

      current_offset ->
        case DatafileEntry.read_raw(datafile.reader, current_offset) do
          {:ok, metadata} -> {metadata, current_offset + metadata.size}
          {:error, _reason} -> nil
        end
    end)
    |> Stream.reject(&is_nil/1)
  end

  ########## Private

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
  @spec file_size(:file.io_device()) :: {:ok, non_neg_integer()} | {:error, atom()}
  defp file_size(io_device) do
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
  require Logger
  import Beetle.Utils

  @typedoc "Type of the key. Beetle only allows string keys"
  @type key_t :: String.t()

  @typedoc """
  Value can be any elixir term, though the interface exposed to the client only
  allows for these value types - string, hash, lists, set, sorted set, bitmaps,
  and bitfields.
  """
  @type value_t :: term()

  @typedoc "Represents an entry stored in the datafile"
  @type t :: %__MODULE__{
          crc: pos_integer(),
          expiration: non_neg_integer(),
          key_size: pos_integer(),
          value_size: pos_integer(),
          key: key_t(),
          value: value_t()
        }

  @typedoc """
  Represents an entry's metadata and location in the datafile. It contains:
  - `:entry`: represents the entry
  - `:is_stale`: if an entry has been deleted or has expired, its considered as
    stale
  - `:size`: total size of the entry in bytes
  - `:position`: byte offset where entry begins in the datafile

  Used for tracking storage details for entries during operations like
  compaction or building keydir.
  """
  @type metadata_t :: %{
          key: key_t(),
          value: value_t(),
          is_stale: boolean(),
          size: non_neg_integer(),
          position: non_neg_integer(),
          expiration: non_neg_integer()
        }

  @header_size 20
  @tombstone_value <<0>>

  defstruct [:crc, :expiration, :key_size, :value_size, :key, :value]

  def tombstone_value, do: @tombstone_value

  @doc "Creates a new serialized entry for storage in the datafile"
  @spec new(key_t(), value_t(), non_neg_integer()) :: binary()
  def new(key, value, expiration) do
    key_size = byte_size(key)
    serialized_value = serialize(value)
    value_size = byte_size(value)

    entry = [<<expiration::64, key_size::32, value_size::32>>, key, serialized_value]
    checksum = :erlang.crc32(entry)
    binary = :erlang.iolist_to_binary(entry)

    <<checksum::32, binary::binary>>
  end

  @doc "Reads and decodes an entry from the datafile at the specified position"
  @spec get(:file.io_device(), non_neg_integer(), non_neg_integer()) ::
          {:ok, t() | nil} | {:error, String.t()}
  def get(io_device, pos, size) do
    with {:ok, binary} <- :file.pread(io_device, pos, size),
         {:ok, entry} <- decode_entry(binary),
         false <- expired?(entry.expiration),
         false <- deleted?(entry.value) do
      {:ok, entry}
    else
      true -> {:ok, nil}
      :eof -> {:error, "EOF_REACHED"}
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
          {:ok, metadata_t()} | {:error, String.t()}
  def read_raw(io_device, pos) do
    with {:ok, <<_::32, _::64, key_size::32, value_size::32>>} <-
           :file.pread(io_device, pos, @header_size),
         total_size <- @header_size + key_size + value_size,
         {:ok, binary} <- :file.pread(io_device, pos, total_size),
         {:ok, entry} <- decode_entry(binary) do
      {:ok,
       %{
         key: entry.key,
         value: entry.value,
         position: pos,
         size: total_size,
         expiration: entry.expiration,
         is_stale: expired?(entry.expiration) or deleted?(entry.value)
       }}
    else
      :eof -> {:error, "EOF_REACHED"}
      {:error, reason} -> {:error, inspect(reason)}
    end
  end

  ############### Private

  @spec decode_entry(binary()) :: {:ok, t()} | {:error, String.t()}
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
      false -> {:error, "ENTRY_INVALID_CHECKSUM"}
      _ -> {:error, "ENTRY_INVALID_FORMAT"}
    end
  end

  defp expired?(0), do: false
  defp expired?(expiration), do: System.system_time(:millisecond) >= expiration

  defp deleted?(@tombstone_value), do: true
  defp deleted?(_), do: false
end
