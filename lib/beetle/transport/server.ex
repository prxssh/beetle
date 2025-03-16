defmodule Beetle.Transport.Server do
  @moduledoc """
  Asynchronous TCP server implementation using GenServer that manages client
  connections.

  The server operates as a GenServer process that listens for incoming TCP
  connections on a specified port. When started, it creates a listening socket
  and begins accepting client connections asychronously.

  When a client connects, the server first accepts the TCP connection, creating
  a new client socket. Then, it starts a new supervised client handler process
  through the `Beetle.Transport.ClientSupervisor`. The socket is configured for
  line-based communication and set to active mode, which means client messages
  will be sent as Erlang messages to the handler process. Finally, ownership of
  the socket is transferred to the client handler process.

  The server maintains its ability to handle multiple clients by immediately
  casting another `:accept` message to itself after setting up each client
  connection. This creates a continuous loop of accepting new connections while
  existing clients are handled by their dedicated processes.
  """
  use GenServer

  require Logger
  alias Beetle.Config
  alias Beetle.Transport.ClientSupervisor

  defstruct [
    :listen_socket,
    :connections,
  ]

  @module __MODULE__

  @max_connections 10
  @accept_timeout :timer.seconds(1)
  @max_restart_frequency :timer.seconds(1)
  @acceptor_pool_size System.schedulers_online() * 2
  @client_socket_options [active: :once, buffer: 65_536, recbuf: 262_144, sndbuf: 262_144]

  @default_socket_options [
    # Receive data as binaries
    :binary,
    # Start in passive mode for controlled message flow
    active: false,
    # Allow socket reuse immediately after shutdown
    reuseaddr: true,
    # Disable Nagle's algorithm for lower latency
    nodelay: true,
    # Keep connections alive
    keepalive: true,
    # Don't delay sending data
    delay_send: false,
    # Send timeout of 30s
    send_timeout: :timer.seconds(30),
    # Close socket if send times out
    send_timeout_close: true,
    # 128KB application buffer
    buffer: 131_072,
    # 512KB OS receive buffer
    recbuf: 524_288,
    # 512KB OS send buffer
    sndbuf: 524_288,
    # Connection backlog queue size
    backlog: 1024
  ]

  # === Client 

  def start_link(_),
    do: GenServer.start_link(@module, nil, name: @module)

  def stop, do: GenServer.call(@module, :stop)

  # === Server

  @impl true
  def init(_) do
    Config.port()
    |> :gen_tcp.listen(@default_socket_options)
    |> case do
      {:ok, listen_socket} ->
        spawn_acceptor_pool(listen_socket, @acceptor_pool_size)

        {:ok,
         %__MODULE__{
           connections: %{},
           listen_socket: listen_socket
         }}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call(:stop, _from, state) do
    cleanup_resources(state)
    :gen_tcp.close(state.listen_socket)

    {:stop, :normal, :ok, state}
  end

  @impl true
  def handle_call(:get_listen_socket, _from, state) do
    {:reply, state.listen_socket}
  end

  @impl true
  def handle_cast({:client_connected, client_pid, client_socket}, state) do
    now = System.system_time(:millisecond)
    ref = Process.monitor(client_pid)

    new_connections =
      Map.put(state.connections, ref, %{pid: client_pid, socket: client_socket, connected_at: now})

    {:noreply, %__MODULE__{state | connections: new_connections}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    case Map.pop(state.connections, ref) do
      {nil, _} ->
        {:noreply, state}

      {_connection, new_connections} ->
        {:noreply, %__MODULE__{state | connections: new_connections}}
    end
  end

  @impl true
  def terminate(_reason, state) do
    cleanup_resources(state)
  end

  # === Private

  defp spawn_acceptor_pool(listen_socket, pool_size) do
    server_pid = self()

    for _ <- 1..pool_size do
      Task.Supervisor.start_child(
        Beetle.TaskSupervisor,
        fn ->
          Process.flag(:priority, :high)
          acceptor_loop(server_pid, listen_socket)
        end,
        restart: :transient
      )
    end
  end

  defp acceptor_loop(server_pid, listen_socket) do
    case :gen_tcp.accept(listen_socket, @accept_timeout) do
      {:ok, client_socket} ->
        handle_new_connection(server_pid, client_socket)
        acceptor_loop(server_pid, listen_socket)

      {:error, :timeout} ->
        acceptor_loop(server_pid, listen_socket)

      {:error, :closed} ->
        :normal

      {:error, reason} ->
        Logger.warning("TCP client accept error: #{inspect(reason)}")

        Process.sleep(@max_restart_frequency)
        acceptor_loop(server_pid, listen_socket)
    end
  end

  defp handle_new_connection(server_pid, client_socket) do
    with {:ok, client_pid} <- ClientSupervisor.start_client(client_socket),
         :ok <- :inet.setopts(client_socket, @client_socket_options),
         :ok <- :gen_tcp.controlling_process(client_socket, client_pid) do
      GenServer.cast(server_pid, {:client_connected, client_pid, client_socket})
    else
      {:error, reason} ->
        :gen_tcp.close(client_socket)
        {:stop, reason, client_socket}
    end
  end

  defp cleanup_resources(state) do
    :gen_tcp.close(state.listen_socket)

    for {_ref, conn} <- state.connections do
      :gen_tcp.close(conn.socket)
    end
  end

  defp format_uptime(ms) do
    seconds = div(ms, 1000)
    minutes = div(seconds, 60)
    hours = div(minutes, 60)
    days = div(hours, 24)

    "#{days}d #{rem(hours, 24)}h #{rem(minutes, 60)}m #{rem(seconds, 60)}s"
  end
end
