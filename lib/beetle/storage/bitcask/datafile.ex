defmodule Beetle.Storage.Bitcask.Datafile do
  @moduledoc """
  Datafile(s) are the primary storage units in a Bitcask database. They are
  append only files that contains the actual key-value data entries. Each
  datafile typically contains a series of entries. An entry usually looks like
  this:
  """

  @typedoc """
  Datafile struct containting:

  * `file_id`: unique identifier for the datafile (monotonically increasing)
  * `writer`: file handle for write operations
  * `reader`: file handler for read operations
  * `offset`: current write position in bytes from start of file

  Each datafile maintains two separate file handles - one optimized for reads
  and another for appends - to prevent seek contention b/w operations.
  """
  alias Beetle.Storage.Bitcask.Datafile.Entry

  @type io_device :: :file.io_device()

  @type t :: %__MODULE__{
          file_id: pos_integer(),
          writer: io_device(),
          reader: io_device(),
          offset: pos_integer()
        }
  defstruct [:file_id, :writer, :reader, :offset]

  @doc """
  Creates a new datafile instance at the given path.

  Takes a unique file_id and a path to create/open the datafile. Opens two file
  handles: one for writing (in append mode) and another for reading.
  """
  @spec new(pos_integer(), String.t()) :: {:ok, t()} | {:error, String.t()}
  def new(file_id, path) do
    file_path = path |> Path.join(get_filename(file_id)) |> to_charlist()

    with {:ok, writer} <- :file.open(file_path, [:append, :binary, :raw]),
         {:ok, reader} <- :file.open(file_path, [:read, :binary, :raw]),
         {:ok, size} <- get_file_size(writer) do
      {:ok, %__MODULE__{file_id: file_id, writer: writer, reader: reader, offset: size}}
    else
      {:error, reason} -> {:error, inspect(reason)}
    end
  end

  @doc """
  Lists all datafiles in the specified directory with their file ids.

  Scans the directory for files matching pattern 'db_*.beetle' and returns a
  list of tuples containing file id and full path for each datafile.
  """
  @spec get_all_datafiles(String.t()) :: [pos_integer()]
  def get_all_datafiles(dir) do
    dir
    |> Path.join("beetle_*.db")
    |> Path.wildcard()
    |> Enum.map(fn path -> path |> Path.basename() |> extract_file_id() end)
  end

  @doc """
  Generates the standard filename for a datafile given its file id.
  """
  @spec get_filename(pos_integer()) :: String.t()
  def get_filename(file_id), do: "beetle_#{file_id}.db"

  @doc """
  Get the file size
  """
  def get_file_size(file) do
    case :file.read_file_info(file) do
      {:ok, {:file_info, size, _, _, _, _, _, _, _, _, _, _, _, _}} -> {:ok, size}
      error -> error
    end
  end

  @doc """
  Flushes any pending writes from buffers to the disk
  """
  @spec sync(t()) :: :ok | {:error, any()}
  def sync(datafile), do: :file.sync(datafile.writer)

  @spec close(t()) :: :ok | {:error, String.t()}
  def close(datafile) do
    with :ok <- :file.close(datafile.writer),
         :ok <- :file.close(datafile.reader) do
      :ok
    else
      {:error, reason} -> {:error, inspect(reason)}
    end
  end

  @doc """
  Reads a binary value from a datafile at specific position.

  Positions the file pointer at the specified offset and reads the given number
  of bytes. The read value is then parsed and validated before being returned.
  """
  @spec get(t(), pos_integer(), pos_integer()) :: {:ok, binary()} | {:error, any()}
  def get(datafile, offset, size) do
    with {:ok, _} <- :file.position(datafile.reader, offset),
         {:ok, raw_entry} <- :file.read(datafile.reader, size),
         {:ok, parsed_entry} <- Entry.decode(raw_entry) do
      {:ok, parsed_entry.value}
    else
      error -> error
    end
  end

  @doc """
  Writes a key-value pair to the datafile with timestamp and CRC validation.
  """
  @spec put(t(), String.t(), any(), non_neg_integer()) :: {:ok, pos_integer()} | {:error, any()}
  def put(datafile, key, value, expiration \\ 0) do
    entry = Entry.encode(key, value, expiration)

    case :file.write(datafile.writer, entry) do
      :ok -> {:ok, datafile.offset + byte_size(entry)}
      error -> error
    end
  end

  @spec dump_all_entries(t()) ::
          {:ok, [%{entry: Entry.t(), size: pos_integer(), pos: pos_integer()}]} | {:error, any()}
  def dump_all_entries(datafile) do
    case :file.position(datafile.reader, 0) do
      {:ok, _} ->
        datafile.reader
        |> stream_entries()
        |> Enum.reduce_while({:ok, []}, &collect_entry/2)
        |> case do
          {:ok, entries} -> {:ok, Enum.reverse(entries)}
          error -> error
        end

      error ->
        error
    end
  end

  defp stream_entries(io_device) do
    Stream.unfold(0, fn offset ->
      case Entry.read_entry(io_device, offset) do
        {:ok, entry, next_offset} ->
          metadata = %{entry: entry, pos: offset, size: next_offset}
          {metadata, next_offset}

        _ ->
          nil
      end
    end)
  end

  defp collect_entry(entry, {:ok, entries}), do: {:cont, {:ok, [entry | entries]}}
  defp collect_entry(_, error), do: {:halt, error}

  @spec extract_file_id(String.t()) :: pos_integer() | nil
  defp extract_file_id(filename) do
    case Regex.run(~r/^beetle_(\d+)\.db$/, filename) do
      [_, id] -> String.to_integer(id)
      _ -> nil
    end
  end
end

defmodule Beetle.Storage.Bitcask.Datafile.Entry do
  @moduledoc """
  Represents and handles entries in a Bitcask datastore.

  An Entry is the fundamental storage unit in Bitcask, containing both data and
  metadata. Each entry is structured as follows:

    +-----------+------------+------------+------------+-----------+-------+-------+
    |  Checksum | Timestamp  | Expiration | Key Size  | Value Size | Key   | Value |
    | (32 bits) | (64 bits)  | (64 bits)  | (32 bits) | (32 bits)  | (var) | (var) |
    +-----------+------------+------------+-----------+-----------+--------+-------+

  Fields description:
  * `checksum` - CRC32 hash of the entry for data integrity verification
  * `timestamp` - Unix timestamp (in seconds) when the entry was created
  * `expiration` - Unix timestamp when the entry expires (0 means never)
  * `key_size` - Size of the key in bytes
  * `value_size` - Size of the value in bytes
  * `key` - The actual key data
  * `value` - The actual value data
  """

  defstruct [:checksum, :timestamp, :expiration, :key_size, :value_size, :key, :value]

  @type t :: %__MODULE__{
          checksum: pos_integer(),
          timestamp: pos_integer(),
          expiration: non_neg_integer(),
          key_size: pos_integer(),
          value_size: pos_integer(),
          key: String.t(),
          value: any()
        }

  @size_checksum 32
  @size_timestamp 64
  @size_key_size 32
  @size_value_size 32

  @header_size div(@size_checksum + @size_timestamp * 2 + @size_key_size + @size_value_size, 8)

  @errors %{
    expired: "EXPIRED",
    end_of_file: "EOF",
    invalid_entry: "INVALID_ENTRY",
    invalid_header: "INVALID_HEADER",
    checksum_mismatch: "CHECKSUM_MISMATCH"
  }

  @spec read_entry(:file.io_device(), non_neg_integer()) :: {:ok, t()} | {:error, any()}
  def read_entry(io_device, offset) do
    with {:ok, header_bin} <- :file.pread(io_device, offset, @header_size),
         {:ok, sizes} <- parse_header_sizes(header_bin),
         total_size = @header_size + sizes.key_size + sizes.value_size,
         {:ok, entry} <- :file.pread(io_device, offset, total_size),
         :ok <- validate_checksum(entry),
         {:ok, parsed_entry} <- decode_raw(entry) do
      {:ok, parsed_entry, offset + total_size}
    else
      :eof -> {:error, @errors.end_of_file}
      error -> error
    end
  end

  @spec encode(String.t(), String.t(), non_neg_integer()) ::
          {:ok, binary()} | {:error, String.t()}
  def encode(key, value, expiration) do
    timestamp = System.system_time(:second)
    {key_size, value_size} = {byte_size(key), byte_size(value)}

    entry =
      <<timestamp::@size_timestamp, expiration::@size_timestamp, key_size::@size_key_size,
        value_size::@size_value_size, key::binary, value::binary>>

    checksum = :erlang.crc32(entry)

    <<checksum::@size_checksum, entry::binary>>
  end

  @spec decode(binary()) :: {:ok, t()} | {:error, String.t()}
  def decode(entry) do
    with {:ok, decoded_entry} <- decode_raw(entry),
         :ok <- validate_checksum(entry),
         :ok <- validate_expiration(decoded_entry.expiration) do
      {:ok, decoded_entry}
    else
      error -> error
    end
  end

  @spec decode_raw(binary()) :: {:ok, t()} | {:error, String.t()}
  defp decode_raw(
         <<checksum::@size_checksum, timestamp::@size_timestamp, expiration::@size_timestamp,
           key_size::@size_key_size, value_size::@size_value_size, key::binary-size(key_size),
           value::binary-size(value_size)>>
       ) do
    {:ok, build_entry(checksum, timestamp, expiration, key_size, value_size, key, value)}
  end

  defp decode_raw(_), do: {:error, @errors.invalid_entry}

  defp validate_expiration(0), do: :ok

  defp validate_expiration(expiration) do
    (System.system_time(:second) >= expiration)
    |> case do
      true -> {:error, @errors.expired}
      false -> :ok
    end
  end

  defp validate_checksum(<<checksum::@size_checksum, entry::binary>>) do
    (checksum == :erlang.crc32(entry))
    |> case do
      true -> :ok
      false -> {:error, @errors.checksum_mismatch}
    end
  end

  defp build_entry(checksum, timestamp, expiration, key_size, value_size, key, value) do
    %__MODULE__{
      checksum: checksum,
      timestamp: timestamp,
      expiration: expiration,
      key_size: key_size,
      value_size: value_size,
      key: key,
      value: value
    }
  end

  defp parse_header_sizes(
         <<_checksum::@size_checksum, _timestamp::@size_timestamp, _expiration::@size_timestamp,
           key_size::@size_key_size, value_size::@size_value_size, _rest::binary>>
       ) do
    {:ok, %{key_size: key_size, value_size: value_size}}
  end

  defp parse_header_sizes(_), do: {:error, @errors.invalid_header}
end
