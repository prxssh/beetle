defmodule Beetle.Storage.Bitcask.Keydir do
  @moduledoc """
  Keydir is an in-memory hash table that stores all the keys present in the
  Bitcask instance and maps it to an offset in the datafile where the log entry
  (value) resides.

  A single entry in keydir looks like this:
          -----------------------------------------------
  key -> | file_id | value_size | value_pos | timestamp |
         ------------------------------------------------

  Here, the metadata contains:
  - `file_id`   : the ID of the datafile containing the value
  - `value_size`: size of the stored value in bytes
  - `value_pos` : offset position in the datafile where the value starts
  - `timestamp` : when the entry was written
  """
  alias Beetle.Config
  alias Beetle.Storage.Bitcask.Datafile

  @type t :: %{key_t() => value_t()}

  @type key_t :: String.t()

  @type value_t :: %{
          file_id: pos_integer(),
          value_pos: pos_integer(),
          timestamp: pos_integer()
        }

  @hints_file "beetle.hints"

  @doc """
  Creates a new keydir, either reading it from the hints file present in the
  storage directory or initializes an empty keydir.
  """
  @spec new(String.t(), %{non_neg_integer() => Datafile.t()}) :: {:ok, t()} | {:error, any()}
  def new(path, datafiles \\ %{}) do
    with hints_file_path <- Path.join(path, @hints_file),
         true <- File.exists?(hints_file_path),
         {:ok, binary} <- :file.read_file(hints_file_path),
         {:ok, keydir} <- deserialize(binary) do
      {:ok, keydir}
    else
      false ->
        datafiles
        |> Enum.reduce_while({:ok, %{}}, fn {file_id, file_handle}, {:ok, keydir} ->
          file_id
          |> build_entries_from_datafile(file_handle)
          |> case do
            {:ok, keydir_entries} -> {:cont, {:ok, Map.merge(keydir, keydir_entries)}}
            {:error, reason} -> {:halt, {:error, reason}}
          end
        end)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Writes the keydir to a disk, creating a .hints file for faster bootups.
  """
  @spec persist(t()) :: :ok | {:error, any()}
  def persist(keydir) do
    path = Config.storage_directory() |> Path.join(@hints_file)

    keydir
    |> serialize()
    |> then(&:file.write_file(path, &1))
    |> case do
      :ok -> :ok
      error -> error
    end
  end

  @doc "Puts a new entry in the keydir"
  @spec put(t(), String.t(), non_neg_integer(), non_neg_integer()) :: t()
  def put(keydir, key, file_id, value_pos) do
    Map.put(keydir, key, %{
      file_id: file_id,
      value_pos: value_pos,
      timestamp: System.system_time(:second)
    })
  end

  @doc """
  Gets an entry from the keydir.

  Returns `nil` if no entry is found
  """
  @spec get(t(), String.t()) :: value_t() | nil
  def get(keydir, key), do: Map.get(keydir, key)

  @doc "List all the keys in the keydir"
  @spec keys(t()) :: [String.t()]
  def keys(keydir), do: Map.keys(keydir)

  # ==== Private

  # Serializes the keydir to binary format.
  #
  #  It comes in handy when we want to persist the keydir to disk after merging is
  #  complete.
  @spec serialize(t()) :: binary()
  defp serialize(keydir) when is_map(keydir), do: :erlang.term_to_binary(keydir)

  @spec deserialize(binary()) :: {:ok, t()} | {:error, :invalid_format}
  defp deserialize(binary) when is_binary(binary) do
    term = :erlang.binary_to_term(binary)

    if valid_keydir?(term), do: {:ok, term}, else: {:error, :invalid_format}
  end

  defp valid_keydir?(map) when is_map(map),
    do: Enum.all?(map, fn {key, value} -> is_binary(key) and valid_value?(value) end)

  defp valid_keydir?(_), do: false

  @spec valid_value?(map()) :: boolean()
  defp valid_value?(value) when is_map(value) do
    required_keys = [:file_id, :value_size, :value_pos, :timestamp]
    has_all_keys? = Enum.all?(required_keys, &Map.has_key?(value, &1))

    has_all_keys? and
      is_integer(value.file_id) and
      value.file_id >= 0 and
      is_integer(value.value_size) and
      value.value_size > 0 and
      is_integer(value.value_pos) and
      value.value_pos >= 0 and
      is_integer(value.timestamp)
  end

  defp valid_value?(_), do: false

  @spec build_entries_from_datafile(non_neg_integer(), :file.io_device()) ::
          {:ok, t()} | {:error, any()}
  defp build_entries_from_datafile(file_id, handle) do
    handle
    |> Datafile.dump_all_entries()
    |> case do
      {:ok, entries} ->
        {:ok,
         entries
         |> Enum.reduce(%{}, fn {offset, entry}, keydir ->
           put(keydir, entry.key, file_id, offset)
         end)}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
