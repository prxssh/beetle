defmodule Beetle.Storage.Bitcask.Keydir do
  @moduledoc """
  Keydir is an in-memory hashtable that serves as an index for all keys in the
  bitcask database. It provides fast lookups for any key in the database,
  significantly speeding up read operations.

  Structure of the keydir is as follows:

             ------------------------------------------------
      key -> | file_id | value_size | value_pos | timestamp |
             ------------------------------------------------

  Here,
  - `file_id`: identifier of the datafile containing the key's most recent value
  - `value_size`: size of the value
  - `value_pos`: offset within the file where the value starts
  - `timestamp`: timestamp at which the value was written
  """
  alias Beetle.Config.State, as: Config

  @type file_id :: non_neg_integer()
  @type value_size :: non_neg_integer()
  @type value_pos :: non_neg_integer()
  @type timestamp :: non_neg_integer()
  @type entry :: {file_id(), value_size(), value_pos(), timestamp()}
  @type t :: %{optional(binary()) => entry()}

  @hints_file_name "beetle.hints"

  @doc """
  Creates a new keydir by loading from a hints file if it exists.
  """
  @spec new(String.t()) :: {:ok, t()} | {:error, term()}
  def new(path) do
    path
    |> load_hints_file()
    |> case do
      {:ok, keydir} -> {:ok, keydir}
      error -> error
    end
  end

  @doc """
  Adds or updates an entry in the keydir.
  """
  @spec put(t(), binary(), file_id(), value_size(), value_pos(), timestamp()) :: {:ok, t()}
  def put(keydir, key, file_id, value_size, value_offset, timestamp) do
    {:ok, Map.put(keydir, key, {file_id, value_size, value_offset, timestamp})}
  end

  @doc """
  Retrieves an entry from the keydir.
  """
  @spec get(t(), binary()) :: {:ok, entry()} | :error
  def get(keydir, key) do
    case Map.fetch(keydir, key) do
      {:ok, entry} -> {:ok, entry}
      :error -> :error
    end
  end

  @doc """
  Removes an entry from the keydir.
  """
  @spec delete(t(), binary()) :: t()
  def delete(keydir, key), do: Map.delete(keydir, key)

  @doc """
  Writes the keydir to a hints file.
  """
  @spec write_hints_file(t(), Path.t()) :: :ok | {:error, term()}
  def write_hints_file(keydir, path) do
    encoded = :erlang.term_to_binary(keydir)

    path
    |> Path.join(@hints_file_name)
    |> to_charlist()
    |> :file.write_file(encoded)
  end

  @spec close(t()) :: :ok | {:error, term()}
  def close(keydir), do: write_hints_file(keydir, Config.get_storage_directory())

  @spec load_hints_file(String.t()) :: {:ok, t()} | {:error, any()}
  defp load_hints_file(path) do
    path
    |> Path.join(@hints_file_name)
    |> File.read()
    |> case do
      {:ok, contents} ->
        {:ok, :erlang.binary_to_term(contents)}

      {:error, :enoent} ->
        {:ok, %{}}

      error ->
        error
    end
  end
end
