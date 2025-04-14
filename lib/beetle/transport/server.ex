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

  @default_server_socket_opts [
    :binary,
    nodelay: true,
    backlog: 1024,
    active: false,
    reuseaddr: true,
    delay_send: false,
    send_timeout_close: true,
    send_timeout: :timer.seconds(30)
  ]

  @default_client_socket_opts [active: :once]

  @accept_timeout :timer.seconds(1)
  @max_restart_frequency :timer.seconds(1)
  @acceptor_pool_size System.schedulers_online() * 2

  defstruct [:socket, :connections]

  ########## Client

  def start_link(_), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  def stop, do: GenServer.call(__MODULE__, :stop)

  ########## Server

  @impl true
  def init(_) do
    case :gen_tcp.listen(Config.port(), @default_server_socket_opts) do
      {:ok, socket} ->
        spawn_acceptor_pool(socket, @acceptor_pool_size)
        {:ok, %__MODULE__{connections: %{}, socket: socket}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_cast({:client_connected, client_pid, client_socket}, state) do
    now = System.system_time(:millisecond)
    ref = Process.monitor(client_pid)

    connection_ref = %{pid: client_pid, socket: client_socket, connected_at: now}
    updated_connection = Map.put(state.connections, ref, connection_ref)

    Logger.debug(
      "#{__MODULE__} tcp client connected, pid: #{client_pid}, total_clients: #{map_size(updated_connection)}"
    )

    {:noreply, %__MODULE__{state | connections: updated_connection}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Map.pop(state.connections, ref) do
      {nil, _} ->
        {:noreply, state}

      {_connection, new_connections} ->
        {:noreply, %__MODULE__{state | connections: new_connections}}
    end
  end

  @impl true
  def handle_call(:stop, _from, state) do
    cleanup_resources(state)
    :gen_tcp.close(state.socket)

    {:stop, :normal, :ok, state}
  end

  @impl true
  def terminate(_reason, state), do: cleanup_resources(state)

  ########## Private

  @spec spawn_acceptor_pool(:gen_tcp.socket(), pos_integer()) :: :ok
  defp spawn_acceptor_pool(socket, pool_size) do
    server_pid = self()

    for _ <- 1..pool_size do
      Task.Supervisor.start_child(
        Beetle.TaskSupervisor,
        fn ->
          Process.flag(:priority, :high)
          acceptor_loop(server_pid, socket)
        end,
        restart: :transient
      )
    end

    :ok
  end

  @spec acceptor_loop(pid(), :gen_tcp.socket()) :: :normal
  defp acceptor_loop(pid, socket) do
    case :gen_tcp.accept(socket, @accept_timeout) do
      {:ok, client_socket} ->
        handle_new_connection(pid, client_socket)
        acceptor_loop(pid, socket)

      {:error, :timeout} ->
        Logger.error("#{__MODULE__} tcp acceptor timed out, pid: #{pid}")

        acceptor_loop(pid, socket)

      {:error, :closed} ->
        Logger.error("#{__MODULE__} tcp acceptor closed, pid: #{pid}")

        :normal

      {:error, reason} ->
        Logger.error("#{__MODULE__} TCP client accept error: #{inspect(reason)}")

        Process.sleep(@max_restart_frequency)
        acceptor_loop(pid, socket)
    end
  end

  @spec handle_new_connection(pid(), :gen_tcp.socket()) :: :ok | :error
  defp handle_new_connection(pid, client_socket) do
    with {:ok, client_pid} <- ClientSupervisor.start_client(client_socket),
         :ok <- :inet.setopts(client_socket, @default_client_socket_opts),
         :ok <- :gen_tcp.controlling_process(client_socket, client_pid) do
      GenServer.cast(pid, {:client_connected, client_pid, client_socket})
    else
      {:error, reason} ->
        Logger.error(
          "#{__MODULE__} failed to handle new connection, pid: #{pid}, error: #{inspect(reason)}"
        )

        :gen_tcp.close(client_socket)
        :error
    end
  end

  @spec cleanup_resources(%__MODULE__{}) :: :ok
  defp cleanup_resources(state) do
    :gen_tcp.close(state.socket)

    for {_, conn} <- state.connections do
      :gen_tcp.close(conn.socket)
    end

    :ok
  end
end
