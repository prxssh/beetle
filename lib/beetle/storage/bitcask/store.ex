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
  Creates a new Bitcask instance with the given directory path. If the
  directory doesn't exist, it will be created.
  """
  @spec new() :: {:ok, t()} | {:error, any()}
  def new do
    with {:ok, keydir} <- Keydir.new(),
         {:ok, datafile_handles} <- read_older_datafiles(),
         active_datafile_id <- map_size(datafile_handles) + 1,
         {:ok, active_datafile_handle} <- active_datafile_id |> Datafile.new() do
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

  # === Private

  @spec read_older_datafiles() :: {:ok, %{file_id_t() => Datafile.t()}} | {:error, String.t()}
  defp read_older_datafiles do
    Config.storage_directory()
    |> Path.join("beetle_*.db")
    |> Path.wildcard()
    |> Enum.reduce_while({:ok, %{}}, fn datafile_path, {:ok, acc} ->
      file_id = extract_file_id_from_path(datafile_path)

      datafile_path
      |> to_charlist()
      |> Datafile.new()
      |> case do
        {:ok, datafile_handle} -> {:cont, {:ok, Map.put(acc, file_id, datafile_handle)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  @spec extract_file_id_from_path(String.t()) :: non_neg_integer()
  defp extract_file_id_from_path(path) do
    case Regex.run(~r/beetle_(\d+)\.db$/, path) do
      [_, file_id] -> String.to_integer(file_id)
      nil -> raise "invalid datafile naming convention"
    end
  end

  @spec create_datafile_path(non_neg_integer()) :: charlist()
  defp create_datafile_path(file_id) do
    Config.storage_directory()
    |> Path.join("beetle_#{file_id}.db")
    |> to_charlist()
  end
end
