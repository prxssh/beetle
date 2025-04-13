defmodule Beetle.Storage.Bitcask.Keydir do
  @moduledoc """
  Keydir is an in-memory hash table that stores all the keys present in the
  Bitcask instance and maps it to an offset in the datafile where the log entry
  (value) resides.

  A single entry in the keydir has the following structure:

             -----------------------------------------------
    key --> | file_id | value_size | value_pos | timestamp |
            -----------------------------------------------

  - `file_id`: the ID of the datafile containing the value
  - `value_size`: size of the store value in bytes
  - `value_pos`: offset position in the datafile where the value resides
  - `timestamp`: unix-time at which the entry was written in the keydir
  """
  import Beetle.Utils
  alias Beetle.Storage.Bitcask.Datafile

  @typedoc "Metadata about value in the datafile"
  @type value_t :: [
          file_id: Datafile.file_id_t(),
          value_pos: non_neg_integer(),
          value_size: non_neg_integer(),
          timestamp: non_neg_integer()
        ]

  @typedoc "Represents the keydir"
  @type t :: %{Datafile.Entry.key_t() => value_t()}

  @hints_file "beetle.hints"

  @spec new(String.t(), Datafile.map_t()) :: {:ok, t()} | {:error, String.t()}
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

  @spec put(t(), String.t(), value_t()) :: t()
  def put(keydir, key, value) do
    value
    |> Keyword.put(:timestamp, System.system_time(:millisecond))
    |> then(&Map.put(keydir, key, &1))
  end

  def get(keydir, key, default_value \\ nil), do: Keyword.get(keydir, key, default_value)

  @spec persist(t(), Path.t()) :: :ok | {:error, term()}
  def persist(keydir, path) do
    path
    |> Path.join(@hints_file)
    |> to_charlist()
    |> :file.write_file(serialize(keydir))
  end

  ########## Private

  @spec validate_keydir(t()) :: :ok | {:error, String.t()}
  defp validate_keydir(keydir) when is_map(keydir) do
    keydir
    |> Enum.all?(&valid_entry?/1)
    |> case do
      true -> :ok
      false -> {:error, "MALFORMED_KEYDIR_FORMAT"}
    end
  end

  defp validate_keydir(_), do: {:error, "MALFORMED_KEYDIR_FORMAT"}

  @spec valid_entry?({Datafile.Entry.key_t(), value_t()}) :: boolean()
  defp valid_entry?({key, value}) when is_binary(key) and is_list(value) do
    required_keys = [:file_id, :value_pos, :value_size]

    with true <- Enum.all?(required_keys, &Keyword.has_key?(value, &1)),
         true <- is_integer(value.file_id) and value.file_id > -1,
         true <- is_integer(value.value_pos) and value.value_pos > -1,
         true <- is_integer(value.value_size) and value.value_size > -1 do
      true
    else
      _ -> false
    end
  end

  defp valid_entry?(_), do: false

  @spec read_from_datafiles(Datafile.map_t()) :: {:ok, t()} | {:error, term()}
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
        {:ok, {file_id, entries_stream}}, {:ok, keydir} ->
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
