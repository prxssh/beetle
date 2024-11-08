defmodule Beetle.Storage.Bitcask.Store do
  @moduledoc """
  Module implementing the Bitcask storage engine.

  The Store manages three main components:

  * KeyDir
   - In-memory index mapping keys to their most recent location on disk
   - Enables O(1) key lookups by tracking file_id and offset for each value
   - Rebuilt on startup by scanning all datafiles

  * Active Datafile
   - Single writable file that receives all new writes
   - Append-only to ensure sequential writes
   - Rotated to stale status when size limit reached

  * Stale Datafiles
   - Read-only files containing historical data
   - Multiple files indexed by file_id
   - Subject to compaction to reclaim space
  """
  alias Beetle.Storage.Bitcask.{
    Keydir,
    Datafile
  }

  @type t :: %__MODULE__{
          keydir: Keydir.t(),
          active_datafile: Datafile.t(),
          stale_datafiles: %{non_neg_integer() => Datafile.t()},
          file_id: pos_integer()
        }
  defstruct [:active_datafile, :stale_datafiles, :keydir, :file_id]

  @spec new(String.t()) :: {:ok, t()} | {:error, String.t()}
  def new(storage_dir) do
    with {:ok, stale_datafiles} <- load_stale_datafiles(storage_dir),
         file_id <- map_size(stale_datafiles),
         {:ok, active_datafile} <- Datafile.new(file_id, storage_dir),
         {:ok, keydir} <- Keydir.new(storage_dir) do
      {:ok,
       %__MODULE__{
         keydir: keydir,
         file_id: file_id,
         active_datafile: active_datafile,
         stale_datafiles: stale_datafiles
       }}
    else
      error -> error
    end
  end

  @spec load_stale_datafiles(String.t()) ::
          {:ok, %{non_neg_integer() => Datafile.t()}} | {:error, String.t()}
  defp load_stale_datafiles(storage_dir) do
    {stale_datafiles, errors} =
      storage_dir
      |> Datafile.get_all_datafiles()
      |> Enum.reduce({%{}, []}, fn file_id, {stale_datafiles, errors} ->
        file_id
        |> Datafile.new(storage_dir)
        |> case do
          {:ok, stale_datafile} ->
            {Map.put(stale_datafiles, file_id, stale_datafile), errors}

          {:error, reason} ->
            {stale_datafiles, [reason | errors]}
        end
      end)

    if Enum.empty?(errors),
      do: {:ok, stale_datafiles},
      else: {:error, inspect(errors)}
  end
end
