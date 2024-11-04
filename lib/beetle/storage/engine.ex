defmodule Beetle.Storage.Engine do
  @moduledoc false
  use GenServer

  @module __MODULE__

  # === Client API

  def start_link(config_opts), do: GenServer.start_link(@module, config_opts, name: @module)

  def get(key), do: GenServer.call(@module, {:get, key})

  def put(key, value, opts \\ nil), do: GenServer.call(@module, {:put, key, value, opts})

  def del(keys), do: GenServer.call(@module, {:delete, List.wrap(keys)})

  # === Server Callbacks

  @impl true
  def init(config_opts) do
    {:ok, %{}}
  end

  @impl true
  def handle_call({:get, key}, state), do: {:reply, {:ok, nil}, state}

  def handle_call({:put, key, value, opts}, state), do: {:reply, {:ok, nil}, state}

  @impl true
  def handle_call({:delete, keys}, state), do: {:reply, :ok, state}
end
