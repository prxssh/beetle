defmodule Beetle.Storage.Bitcask.Keydir do
  @moduledoc """
  Keydir is an in-memory hash table that stores all the keys present in the
  Bitcask instance and maps it to an offset in the datafile where the log entry
  (value) resides.

  A single entry in keydir looks like this:
         ------------------------------------
  key -> | file_id | value_size | value_pos |
         ------------------------------------

  Here, the metadata contains:
  - `file_id`   : the ID of the datafile containing the value
  - `value_size`: size of the stored value in bytes
  - `value_pos` : offset position in the datafile where the value starts

  Generally, keydir also store timestamp but we don't have any need for it now.
  """
  import Beetle.Utils
  alias Beetle.Storage.Bitcask.Datafile

  @typedoc "Metadata about stored value in the datafile"
  @type value_t :: %{
          file_id: non_neg_integer(),
          value_pos: non_neg_integer(),
          value_size: non_neg_integer()
        }

  @typedoc "Represents the keydir"
  @type t :: %{Datafile.Entry.key_t() => value_t()}

  @hints_file "beetle.hints"

  @doc """
  Creates a keydir, either reading from the hints file present at `path` or by
  reading the entries from the older `datafiles`.
  """
  @spec new(String.t(), Bitcask.file_handle_t()) :: {:ok, t()} | {:error, any()}
  def new(path, datafiles \\ %{}) do
    with hints_file_path <- path |> Path.join(@hints_file) |> to_charlist(),
         true <- File.exists?(hints_file_path),
         {:ok, binary} <- :file.read_file(hints_file_path),
         {:ok, keydir} <- deserialize(binary),
         :ok <- validate_keydir(keydir) do
      {:ok, keydir}
    else
      false -> read_from_datafiles(datafiles)
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Writes the keydir to disk, creating a .hints file for faster bootups."
  @spec persist(t(), Path.t()) :: :ok | {:error, any()}
  def persist(keydir, path) do
    path
    |> Path.join(@hints_file)
    |> to_charlist()
    |> :file.write(serialize(keydir))
  end

  @doc "Puts a new entry in the keydir"
  @spec put(t(), String.t(), value_t()) :: t()
  def put(keydir, key, value), do: Map.put(keydir, key, value)

  @doc "Gets the value stored for `key`."
  @spec get(t(), String.t()) :: value_t() | nil
  def get(keydir, key), do: Map.get(keydir, key)

  # === Private

  @spec validate_keydir(t()) :: :ok | {:error, :invalid_keydir_format}
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
    required_keys = [:file_id, :value_pos, :value_size]

    with true <- Enum.all?(required_keys, &Map.has_key?(value, &1)),
         true <- is_integer(value.file_id) and value.file_id > -1,
         true <- is_integer(value.value_pos) and value.value_pos > -1,
         true <- is_integer(value.value_size) and value.value_size > -1 do
      true
    else
      _ -> false
    end
  end

  @spec read_from_datafiles(Datafile.map_t()) :: {:ok, t()} | {:error, any()}
  defp read_from_datafiles(datafiles) do
    datafiles
    |> Task.async_stream(
      fn {file_id, datafile} -> {file_id, Datafile.scan_valid_entries(datafile)} end,
      ordered: false,
      timeout: :timer.seconds(15),
      max_concurrency: System.schedulers_online()
    )
    |> Enum.reduce_while(
      {:ok, %{}},
      fn
        {file_id, entries_stream}, {:ok, keydir} ->
          new_keydir =
            entries_stream
            |> Enum.reduce(keydir, fn %{key: key, position: position, size: size}, acc ->
              Map.put(acc, key, %{file_id: file_id, value_pos: position, value_size: size})
            end)

          {:cont, {:ok, new_keydir}}

        {:exit, reason}, _acc ->
          {:halt, {:error, {:task_failed, reason}}}
      end
    )
  end
end
