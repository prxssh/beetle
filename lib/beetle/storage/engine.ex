defmodule Beetle.Storage.Engine do
  @moduledoc """
  GenServer process that manages the lifecycle of a Bitcask storage instance.

  This engine wraps the core Bitcask storage functionality and handles:

  * Basic Operations
   - Get: Fetch value by key
   - Put: Store key-value pair
   - Delete: Remove key(s)

  * Background Tasks
   - Sync: Periodically flush data to disk
   - Log Rotation: Create new datafile when size limit reached
   - Compaction: Merge and cleanup old datafiles
  """
  use GenServer

  alias Beetle.Storage.Bitcask.{
    Store,
    Operations
  }

  alias Beetle.Config.State, as: Config

  @module __MODULE__

  # === Client API

  def start_link(_), do: GenServer.start_link(@module, nil, name: @module)

  def get(key), do: GenServer.call(@module, {:get, key})

  def put(key, value, opts \\ %{}), do: GenServer.call(@module, {:put, key, value, opts})

  def del(keys), do: GenServer.call(@module, {:delete, List.wrap(keys)})

  # === Server Callbacks

  @impl true
  def init(_) do
    Config.get_storage_directory()
    |> Store.new()
    |> case do
      {:ok, store} -> {:ok, store}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:get, key}, _, store) do
    case Operations.get(store, key) do
      {:ok, value} -> {:reply, {:ok, value}, store}
      :error -> {:reply, {:error, :failed}, store}
      {:error, :expired} -> {:reply, {:error, :expired}, store}
      {:error, reason} -> {:reply, {:error, reason}, store}
    end
  end

  @impl true
  def handle_call({:put, key, value, opts}, _, store) do
    expiration = Map.get(opts, :expiration, 0)

    case Operations.put(store, key, value, expiration) do
      {:ok, new_store} -> {:reply, :ok, new_store}
      {:error, reason} -> {:reply, {:error, reason}, store}
    end
  end

  @impl true
  def handle_call({:delete, keys}, _, store) do
    {deleted, new_store} =
      keys
      |> Enum.reduce({0, nil}, fn key, {deleted, updated_store} ->
        case Operations.delete(store, key) do
          {:ok, new_store} -> {deleted + 1, new_store}
          _error -> {deleted, updated_store}
        end
      end)

    case new_store do
      nil -> {:reply, deleted, store}
      new_store -> {:reply, deleted, new_store}
    end
  end

  @impl true
  def handle_info(:sync, store) do
    Operations.sync(store)

    schedule_next_sync()
    {:noreply, store}
  end

  @impl true
  def handle_info(:compaction, store) do
    updated_store =
      case Operations.merge(store) do
        {:ok, new_store} -> new_store
        _error -> store
      end

    schedule_next_compaction()
    {:noreply, updated_store}
  end

  @impl true
  def handle_info(:log_rotation, store) do
    updated_store =
      case Operations.log_rotation(store) do
        {:ok, new_store} -> new_store
        _error -> store
      end

    schedule_next_log_rotation()
    {:noreply, updated_store}
  end

  # === Helpers

  defp schedule_next_sync do
    interval = Config.get_sync_interval()
    Process.send_after(self(), interval, :sync)
  end

  defp schedule_next_compaction do
    interval = Config.get_merge_interval()
    Process.send_after(self(), interval, :compaction)
  end

  defp schedule_next_log_rotation do
    interval = :timer.minutes(5)
    Process.send_after(self(), interval, :log_rotation)
  end
end
