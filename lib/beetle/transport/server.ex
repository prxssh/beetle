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
    :acceptor_refs,
    :active_acceptors,
    :max_acceptors,
    :connections,
    :stats
  ]

  @module __MODULE__

  @recv_timeout :timer.minutes(1)
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
    backlog: 1024,
    raw: [
      # Start keepalive after 60s
      {:inet, :tcp_keepidle, 60},
      # Check every 30s
      {:inet, :tcp_keepintvl, 30},
      # 3 retries
      {:inet, :tcp_keepcnt, 3},
      # Allow many pending connections
      {:inet, :tcp_max_syn_backlog, 8192},
      # Fast connection cleanup
      {:inet, :tcp_fin_timeout, 10}
    ]
  ]

  # === Client 

  def start_link(_),
    do: GenServer.start_link(@module, nil, name: @module)

  # === Server

  @impl true
  def init(_) do
    Config.port()
    |> :gen_tcp.listen(@default_socket_options)
    |> case do
      {:ok, listen_socket} ->
        acceptor_refs = :ets.new(:acceptor_refs, [:set, :private])
        spawn_acceptors(listen_socket, System.schedulers_online(), acceptor_refs)
        timer_ref = Process.send_after(self(), :telemetry_tick, :timer.seconds(15))

        {:ok,
         %__MODULE__{
           connections: %{},
           stats: %{
             connection_rate: 0,
             peak_connections: 0,
             connection_count: 0,
             max_connections: 500,
             telemetry_timer: timer_ref,
             start_time: System.monotonic_time(:millisecond)
           },
           listen_socket: listen_socket,
           acceptor_refs: acceptor_refs,
           target_acceptors: System.schedulers_online()
         }}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call(:stop, _from, listen_socket) do
    :gen_tcp.close(listen_socket)
    {:stop, :normal, :ok, listen_socket}
  end

  @impl true
  def handle_cast({:client_connected, client_pid, client_socket}, state) do
    ref = Process.monitor(client_pid)

    now = System.monotonic_time(:millisecond)
    new_count = state.stats.connection_count + 1
    new_peak = max(new_count, state.peak_connections)

    new_connections =
      Map.put(state.connections, ref, %{pid: client_pid, socket: client_socket, connected_at: now})

    new_stats = %{state.stats | peak_connections: new_peak, connection_count: new_count}

    {:noreply, %__MODULE__{state | connections: new_connections, stats: new_stats}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    case Map.pop(state.connections, ref) do
      {nil, _} -> {:noreply, state}

      {_connection, new_connections} -> 
        new_stats = %{state.stats | connection_count: state.stats.connection_count - 1}
        {:noreply, %__MODULE__{state | connections: new_connections, stats: new_stats}}
    end
  end

  # === Private

  defp spawn_acceptors(listen_socket, refs) do
    server_pid = self()

    {pid, ref} =
      spawn_monitor(fn ->
        Process.flag(:priority, :high)
        GenServer.cast(server_pid, {:acceptor_started, ref})
        acceptor_loop(server_pid, listen_socket)
      end)

    :ets.insert(refs, {ref, pid})

    ref
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

        Process.sleep(:timer.seconds(1))
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
        {:stop, reason, listen_socket}
    end
  end
end
