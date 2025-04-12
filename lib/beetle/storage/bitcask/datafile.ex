defmodule Beetle.Storage.Bitcask.Datafile do
  @moduledoc """
  Datafile is an append-only log file that holds the key-value pair along with
  some metadata. 

  A single Bitcask instance could have many datafiles, out of which only one
  will be active and opened for writing, while the other are considered
  immutable and are only used for reads. When the active datafile meets a size
  threshold, it is closed and a new active datafile is created.
  """
  alias Beetle.Storage.Bitcask.Datafile.Entry, as: DatafileEntry

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

  defstruct [:writer, :reader, :offset]

  @doc """
  Opens all the datafile(s) at `path` for reading. 

  This is particularly useful at the database bootup when we need to load all
  other datafiles.
  """
  @spec open(Path.t()) :: {:ok, map_t()} | {:error, String.t()}
  def open(path) do
  end

  @doc """
  Opens a datafile at `path` with both read and write access.

  The file is opened in raw mode with buffered I/O for performance.
  """
  @spec new(Path.t()) :: {:ok, t()} | {:error, String.t()}
  def new(path) do
  end

  @doc "Closes both read and write handles for the datafile"
  @spec close(t()) :: :ok | {:error, String.t()}
  def close(datafile) do
  end

  @doc "Flushes any pending writes to disk"
  @spec sync(t()) :: :ok | {:error, any()}
  def sync(datafile), do: :file.sync(datafile.writer)

  @doc "Constructs a full path for a datafile with the given `file_id`."
  @spec path(String.t(), pos_integer()) :: String.t()
  def path(path, file_id), do: Path.join(path, "beetle_#{file_id}.db")

  @doc "Get an entry from the datafile stored at `pos` having `size`."
  @spec get(t(), non_neg_integer(), non_neg_integer()) :: {:ok, t()} | {:error, any()}
  def get(datafile, pos, size) do
  end

  @doc """
  Writes a new entry to the datafile with the key, value, and expiration.

  Returns the datafile with updated write offset upon success.
  """
  @spec write(t(), DatafileEntry.key_t(), DatafileEntry.value_t(), non_neg_integer()) ::
          {:ok, t()} | {:error, any()}
  def write(datafile, key, value, expiration) do
  end
end

defmodule Beetle.Storage.Bitcask.Datafile.Entry do
  @moduledoc """
  Represents an entry in a Bitcask Datafile.

  Each entry in the datafile has the following format: 

     ---------------------------------------------------------
    | crc | expiration | key_size | value_size | key | value |
    ---------------------------------------------------------

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

  defstruct [:crc, :expiration, :key_size, :value_size, :key, :value]

  @doc "Creates a new serialized entry for storage in the datafile"
  @spec new(key_t(), value_t(), non_neg_integer()) :: binary()
  def new(key, value, expiration) do
  end

  @doc "Reads and decodes an entry from the datafile at the specified position"
  @spec get(:file.io_device(), non_neg_integer(), non_neg_integer()) ::
          {:ok, t()} | {:error, String.t()}
  def get(io_device, pos, size) do
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
  end
end
