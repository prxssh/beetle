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
    storage_path = Config.storage_directory()

    with {:ok, keydir} <- Keydir.new(),
         {:ok, datafile_handles} <- Datafile.open_datafiles(storage_path),
         active_datafile_id <- map_size(datafile_handles) + 1,
         {:ok, active_datafile_handle} <-
           storage_path |> Datafile.get_name(active_datafile_id) |> Datafile.new() do
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
end
