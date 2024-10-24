defmodule Beetle.Server.Core do
  @moduledoc """
  TCP server implementation that accepts client connections and manages client
  handlers. 

  This module serves as the main TCP acceptor process, implementing the
  following features:
  - TCP socket listening and connection acceptance
  - Dynamic supervison of client handlers
  - Automatic recovery mechanism of failed accepts
  - Connection tracking and management
  """
  use GenServer

  require Logger

  alias Beetle.Server.TCP

  @client_supervisor Beetle.ClientSupervisor

  # === Client

  def start_link(_),
    do: GenServer.start_link(__MODULE__, %{socket: %{}, clients: []}, name: __MODULE__)

  # === Server

  @impl true
  def init(state) do
    case TCP.listen(6969) do
      {:ok, socket} ->
        Logger.info("#{__MODULE__}: started tcp server on 6969")

        {:ok, %{state | socket: socket}, {:continue, :accept}}

      {:error, reason} ->
        Logger.error("#{__MODULE__}: failed to start tcp server: #{inspect(reason)}")

        {:stop, reason}
    end
  end

  @impl true
  def handle_continue(:accept, %{socket: socket, clients: clients} = state) do
    with {:ok, client_socket} <- TCP.accept(socket),
         {:ok, pid} <-
           DynamicSupervisor.start_child(
             @client_supervisor,
             {Beetle.Server.ClientHandler, socket: client_socket}
           ),
         :ok <- TCP.set_controlling_process(client_socket, pid) do
      {:noreply, %{state | clients: [pid | clients]}, {:continue, :accept}}
    else
      {:error, reason} ->
        Logger.error("#{__MODULE__}: failed to accept client: #{inspect(reason)}")

        Process.send_after(self(), :retry_accept, :timer.seconds(5))
        {:noreply, state}
    end
  end

  @impl true
  def handle_info(:retry_accept, state), do: {:noreply, state, {:continue, :accept}}
end

defmodule Beetle.Server.ClientHandler do
  @moduledoc """
  GenServer implementation for handling individual TCP client connections.
  """
  use GenServer

  require Logger

  alias Beetle.Server.TCP

  # === Client

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary
    }
  end

  def start_link(socket: client_socket),
    do: GenServer.start_link(__MODULE__, client_socket)

  # === Server

  @impl true
  def init(client_socket) do
    dbg(client_socket)
    Logger.notice("#{__MODULE__}: client connected")

    {:ok, client_socket}
  end

  @impl true
  def handle_info({:tcp, socket, packet}, socket) do
    dbg(packet)

    case TCP.write(packet, socket) do
      :ok ->
        {:noreply, socket}

      {:error, reason} ->
        Logger.error("#{__MODULE__}: write failed: #{inspect(reason)}")

        {:stop, :normal, socket}
    end
  end

  @impl true
  def handle_info({:tcp_closed, socket}, socket) do
    dbg(socket)
    Logger.error("#{__MODULE__}: client disconnected")

    {:stop, :normal, socket}
  end

  @impl true
  def handle_info({:tcp_error, socket, reason}, socket) do
    Logger.error("#{__MODULE__}: client error: #{inspect(reason)}")

    {:stop, :normal, socket}
  end

  @impl true
  def terminate(_reason, socket), do: TCP.close(socket)
end
