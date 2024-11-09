defmodule Beetle.Storage.Bitcask.Datafile do
  @moduledoc """
  Datafile(s) are the primary storage units in a Bitcask database. They are
  append only files that contains the actual key-value data entries. Each
  datafile typically contains a series of entries. An entry usually looks like
  this:

      --------------------------------------------------------
      | crc | timestamp | key_size | value_size | key | value | 
      --------------------------------------------------------

  Here, 
  - `crc`: 32 bit hash of the entry for integrity checks
  - `timestmap`: time of writing, used internally only
  - `key_size`: size of the key in bytes
  - `value_size`: size of the value in bytes
  - `key`: actual key
  - `value`: actual value
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
  @type t :: %__MODULE__{
          file_id: pos_integer(),
          writer: pid(),
          reader: pid(),
          offset: pos_integer()
        }
  defstruct [:file_id, :writer, :reader, :offset]

  @type entry_t :: %{
          crc: integer(),
          expiration: non_neg_integer(),
          timestamp: pos_integer(),
          key_size: pos_integer(),
          value_size: pos_integer(),
          key: binary(),
          value: binary()
        }

  @size_crc 32
  @size_timestamp 64
  @size_key_size 32
  @size_value_size 32

  @doc """
  Creates a new datafile instance at the given path.

  Takes a unique file_id and a path to create/open the datafile. Opens two file
  handles: one for writing (in append mode with delayed writes) and another for
  reading.
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
  @spec get(t(), pos_integer(), pos_integer()) :: {:ok, binary()} | {:error, Atom.t()}
  def get(datafile, offset, size) do
    with {:ok, _} <- :file.position(datafile.reader, offset),
         {:ok, value} <- :file.read(datafile.reader, size) do
      parse_and_validate_entry(value)
    else
      error -> error
    end
  end

  @doc """
  Writes a key-value pair to the datafile with timestamp and CRC validation.
  """
  @spec put(t(), String.t(), any(), non_neg_integer()) :: {:ok, pos_integer()} | {:error, any()}
  def put(datafile, key, value, expiration \\ 0) do
    timestamp = System.system_time(:second)
    {key_size, value_size} = {byte_size(key), byte_size(value)}

    entry =
      <<timestamp::@size_timestamp, expiration::@size_timestamp, key_size::@size_key_size,
        value_size::@size_value_size, key::binary, value::binary>>

    crc = :erlang.crc32(entry)
    full_entry = <<crc::32, entry::binary>>
    entry_size = byte_size(full_entry)

    case :file.write(datafile.writer, full_entry) do
      :ok ->
        dbg(datafile.offset)
        new_offset = datafile.offset + entry_size
        {:ok, datafile.offset + new_offset}

      error ->
        error
    end
  end

  @spec extract_file_id(String.t()) :: pos_integer() | nil
  defp extract_file_id(filename) do
    case Regex.run(~r/^beetle_(\d+)\.db$/, filename) do
      [_, id] -> String.to_integer(id)
      _ -> nil
    end
  end

  @spec parse_and_validate_entry(binary()) :: {:ok, any()} | {:error, atom()}
  defp parse_and_validate_entry(<<crc::@size_crc, entry_data::binary>>) do
    with true <- crc == :erlang.crc32(entry_data),
         {:ok, value} <- get_entry(entry_data) do
      {:ok, value}
    else
      false -> {:error, :invalid_checksum}
      error -> error
    end
  end

  @spec get_entry(binary()) :: {:ok, any()} | {:error, atom()}
  defp get_entry(<<entry_data::binary>>) do
    expired? = fn
      0 -> false
      expiration -> System.system_time(:second) >= expiration
    end

    case entry_data do
      <<_timestamp::@size_timestamp, expiration::@size_timestamp, key_size::@size_key_size,
        value_size::@size_value_size, _key::binary-size(key_size),
        value::binary-size(value_size)>> ->
        if expired?.(expiration), do: {:error, :key_expired}, else: {:ok, value}

      _ ->
        {:error, :invalid_entry}
    end
  end
end
