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
  import Beetle.Utils
  alias Beetle.Storage.Bitcask

  @type key_t :: String.t()
  @type value_t :: %{
          file_id: non_neg_integer(),
          value_pos: non_neg_integer(),
          value_size: non_neg_integer()
        }

  @type t :: %{key_t() => value_t()}

  @hints_file "beetle.hints"

  @doc """
  Creates a keydir, either reading form thie hints file located at path, or by
  reading the raw entries from the datafiles.
  """
  @spec new(String.t(), Bitcask.file_handle_t()) :: {:ok, t()} | {:error, any()}
  def new(path, datafiles \\ %{}) do
    hints_path = Path.join(path, @hints_file)

    with true <- File.exists?(hints_path),
         {:ok, binary} <- :file.read_file(hints_path),
         {:ok, keydir} <- deserialize(binary),
         :ok <- validate_keydir(keydir) do
      {:ok, keydir}
    else
      false -> build_from_datafiles(datafiles)
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Writes the keydir to disk, creating a .hints file for faster bootups.
  """
  @spec persist(t(), Path.t()) :: :ok | {:error, any()}
  def persist(keydir, path) do
    path
    |> Path.join(@hints_file)
    |> :file.write(serialize(keydir))
  end

  @doc "Puts a new entry in the keydir"
  @spec put(t(), String.t(), value_t()) :: t()
  def put(keydir, key, value), do: Map.put(keydir, key, value)

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

  @spec validate_keydir(t()) :: :ok | {:error, atom()}
  defp validate_keydir(keydir) when is_map(keydir) do
    keydir
    |> Enum.all?(&valid_entry?/1)
    |> case do
      true -> :ok
      false -> {:error, :invalid_keydir_format}
    end
  end

  defp validate_keydir(_), do: {:error, :invalid_keydir_format}

  @spec valid_entry?({key_t(), value_t()}) :: boolean()
  defp valid_entry?({key, value}) when is_binary(key) and is_map(value) do
    required_keys = [:file_id, :value_pos, :value_size, :timestamp]

    with true <- Enum.all?(required_keys, &Map.has_key?(value, &1)),
         true <- is_integer(value.file_id) and value.file_id > -1,
         true <- is_integer(value.value_pos) and value.value_pos > -1,
         true <- is_integer(value.value_size) and value.value_size > -1 do
      true
    else
      _ -> false
    end
  end

  @spec build_from_datafiles(Bitcask.file_handle_t()) :: {:ok, t()} | {:error, any()}
  defp build_from_datafiles(datafiles) do
    datafiles
    |> Enum.reduce_while({:ok, %{}}, fn {file_id, datafile}, {:ok, keydir} ->
      datafile.reader
      |> Bitcask.Datafile.Entry.dump_all(0, datafile.offset)
      |> case do
        {:ok, entries} ->
          # TODO: not working correctly
          updated_keydir =
            entries
            |> Enum.reduce(keydir, fn %{pos: pos, size: size, entry: entry}, acc ->
              Map.put(keydir, entry.key, %{
                value_pos: pos,
                value_size: size,
                file_id: file_id
              })
            end)

          {:cont, {:ok, updated_keydir}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
  end
end
