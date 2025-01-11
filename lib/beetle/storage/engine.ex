defmodule Beetle.Storage.Engine do
  @moduledoc """
  GenServer for interacting with the Bitcask store.

  This module provides a process-based interface to a Bitcask key-value store,
  handling concurrent access and maintaining the store's state. It manages
  operations like:

  - Reading and writing key-value pairs
  - Deleting keys
  - Automatic log rotation when file size reaches size thresholds
  - Periodic merging of log files to reclaim space
  - Graceful shutdown ensuring data persistence
  """
  use GenServer

  alias Beetle.Config
  alias Beetle.Storage.Bitcask

  @module __MODULE__

  # === Client 

  def start_link(_), do: GenServer.start_link(@module, nil, name: @module)

  def get(key), do: GenServer.call(@module, {:get, key})

  def set(key, value, expiration), do: GenServer.call(@module, {:put, key, value, expiration})

  def delete(keys), do: GenServer.call(@module, {:delete, keys})

  # === Server

  @impl true
  def init(_) do
    case Bitcask.new() do
      {:ok, store} -> {:ok, store}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:get, key}, _from, store), do: {:reply, Bitcask.get(store, key), store}

  @impl true
  def handle_call({:put, key, value, expiration}, _from, store) do
    store
    |> Bitcask.put(key, value, expiration)
    |> case do
      {:ok, updated_store} -> {:noreply, updated_store}
      {:error, reason} -> {:reply, {:error, reason}, store}
    end
  end

  @impl true
  def handle_call({:delete, keys}, _from, store) do
    {updated_store, count_deleted_keys} = Bitcask.delete(store, keys)
    {:reply, count_deleted_keys, updated_store}
  end

  # === Private
end
