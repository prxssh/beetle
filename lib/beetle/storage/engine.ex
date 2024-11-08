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

  alias Beetle.Storage.Bitcask.Store

  @module __MODULE__

  # === Client API

  def start_link(config_opts), do: GenServer.start_link(@module, config_opts, name: @module)

  def get(key), do: GenServer.call(@module, {:get, key})

  def put(key, value, opts \\ nil), do: GenServer.call(@module, {:put, key, value, opts})

  def del(keys), do: GenServer.call(@module, {:delete, List.wrap(keys)})

  # === Server Callbacks

  @impl true
  def init(config_opts) do
    config_opts.storage_dir
    |> Store.new()
    |> case do
      {:error, reason} ->
        {:stop, reason}

      {:ok, store} ->
        {:ok, %{store: store, config: config_opts}, {:continue, :start_workers}}
    end
  end

  @impl true
  def handle_continue(:start_workers, state) do
  end

  @impl true
  def handle_call({:get, key}, state), do: {:reply, {:ok, nil}, state}

  @impl true
  def handle_call({:put, key, value, opts}, state), do: {:reply, {:ok, nil}, state}

  @impl true
  def handle_call({:delete, keys}, state), do: {:reply, :ok, state}
end
