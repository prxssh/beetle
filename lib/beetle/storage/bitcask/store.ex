defmodule Beetle.Storage.Bitcask do
  @moduledoc """
  Bitcask is a log-structured key-value store designed to handle
  production-grade traffic. 

  It uses a lot of principles from log-structured file systems and draws
  inspiration from a number of designs that involve log file merging. It
  essentially is just a directory of append-only files with a fixed structure
  and an in-memory index holding the keys mapped to a bunch of information
  necessary for point lookups.
  """
  alias Beetle.Config

  alias Beetle.Storage.Bitcask.{
    Keydir,
    Datafile
  }

  @type file_id_t :: non_neg_integer()

  @type t :: %__MODULE__{
          keydir: Keydir.t(),
          active_file: file_id_t(),
          file_handles: %{file_id_t() => Datafile.t()}
        }

  defstruct(
    keydir: nil,
    active_file: 0,
    file_handles: nil
  )

  @doc """
  Creates a new Bitcask instance at the given path, creating the directory if
  it doesn't already exist.
  """
  @spec new(Path.t()) :: {:ok, t()} | {:error, any()}
  def new(path) do
    with :ok <- ensure_created(path),
         {:ok, keydir} <- Keydir.new(path),
         {:ok, datafile_handles} <- Datafile.open_datafiles(path),
         active_datafile_id <- map_size(datafile_handles) + 1,
         {:ok, active_datafile_handle} <-
           path |> Datafile.get_name(active_datafile_id) |> Datafile.new() do
      {:ok,
       %__MODULE__{
         keydir: keydir,
         active_file: active_datafile_id,
         file_handles: Map.put(datafile_handles, active_datafile_id, active_datafile_handle)
       }}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Closes the store after we're done with it.

  It does the following tasks:
  - Persists keydir to disk
  - Sync any pending writes to disk
  - Close all file handles
  """
  @spec close(t()) :: :ok | {:error, any()}
  def close(store) do
    with :ok <- Keydir.persist(store.keydir),
         :ok <- store.file_handles |> Map.get(:active_file) |> Datafile.sync(),
         :ok <-
           store.file_handles
           |> Enum.reduce_while(:ok, fn {_, file_handle}, acc ->
             file_handle
             |> Datafile.close()
             |> case do
               :ok -> {:cont, acc}
               {:error, reason} -> {:halt, {:error, reason}}
             end
           end) do
      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  end

  # === Private

  @spec ensure_created(String.t()) :: :ok | {:error, any()}
  defp ensure_created(path) do
    with false <- File.exists?(path),
         :ok <- File.mkdir_p(path) do
      :ok
    else
      true -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
