defmodule Beetle.Server.ClientSupervisor do
  @moduledoc """
  Dynamically supervises TCP client connection processes.

  Uses `DynamicSupervisor` to manage incoming TCP clients, creating new
  processes as client connects and cleaning them up when they disconnect.
  Implements a one-for-one supervisor strategy to isolate failures between
  client connections.
  """
  use DynamicSupervisor

  # === Client

  def start_link(_), do: DynamicSupervisor.start_link(__MODULE__, nil, name: __MODULE__)

  @doc """
  Starts a supervised client handler process for the given socket.
  """
  @spec start_client(:gen_tcp.socket()) :: {:ok, pid()} | {:error, any()}
  def start_client(socket) do
    child_spec = {Beetle.Server.ClientHandler, socket}
    DynamicSupervisor.start_child(__MODULE__, child_spec)
  end

  # === Server

  @impl true
  def init(_), do: DynamicSupervisor.init(strategy: :one_for_one)
end

defmodule Beetle.Server.ClientHandler do
  @moduledoc """
  Handles individual TCP client connections.

  Manages the communication with a single TCP client, processing incoming
  messages and sending messages. Each instance handles one client socket in
  active mode, receiving TCP messages as Erlang messages. 
  """

  use GenServer
  require Logger

  # === Client

  def start_link(socket), do: GenServer.start_link(__MODULE__, socket)

  # === Server

  @impl true
  def init(socket), do: {:ok, socket}

  @impl true
  def handle_info({:tcp, _socket, data}, socket) do
    handle_received_data(data, socket)

    {:noreply, socket}
  end

  @impl true
  def handle_info({:tcp_closed, _socket}, socket) do
    {:stop, :normal, socket}
  end

  @impl true
  def handle_info({:tcp_error, _socket, reason}, socket) do
    Logger.error("TCP error: #{inspect(reason)}")
    {:stop, reason, socket}
  end

  # === Private

  defp handle_received_data(data, client_socket) do
    data = String.trim(data)
    Logger.info("Received from client: #{data}")

    :gen_tcp.send(client_socket, "ACK\n")
  end
end
