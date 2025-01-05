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
  @type t :: %{key_t() => value_t()}

  @type key_t :: String.t()

  @type value_t :: %{
          file_id: pos_integer(),
          value_size: pos_integer(),
          value_pos: pos_integer(),
          timestamp: pos_integer()
        }

  @doc "Creates a new empty keydir"
  def new, do: %{}

  @doc """
  Serializes the keydir to binary format.

  It comes in handy when we want to persist the keydir to disk after merging is
  complete.
  """
  @spec serialize(t()) :: binary()
  def serialize(keydir) when is_map(keydir), do: :erlang.term_to_binary(keydir)

  @spec deserialize(binary()) :: {:ok, t()} | {:error, :invalid_format}
  def deserialize(binary) when is_binary(binary) do
    term = :erlang.binary_to_term(binary)

    if valid_keydir?(term), do: {:ok, term}, else: {:error, :invalid_format}
  end

  @doc "Puts a new entry in the keydir "
  @spec put(t(), String.t(), pos_integer(), pos_integer(), pos_integer()) :: t()
  def put(keydir, key, file_id, value_size, value_pos) do
    Map.put(keydir, key, %{
      file_id: file_id,
      value_size: value_size,
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
end
