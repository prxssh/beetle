defmodule Beetle.Server do
  @moduledoc """
  Asynchronous TCP server implementation using GenServer that manages client
  connections.

  The server operates as a GenServer process that listens for incoming TCP
  connections on a specified port. When started, it creates a listening socket
  and begins accepting client connections asychronously.

  When a client connects, the server first accepts the TCP connection, creating
  a new client socket. Then, it starts a new supervised client handler process
  through the `Beetle.Server.ClientSupervisor`. The socket is configured for
  line-based communication and set to active mode, which means client messages
  will be sent as Erlang messages to the handler process. Finally, ownership of
  the socket is transferred to the client handler process.

  The server maintains its ability to handle multiple clients by immediately
  casting another `:accept` message to itself after setting up each client
  connection. This creates a continuous loop of accepting new connections while
  existing clients are handled by their dedicated processes.

  The server can be started by:

    # Starts with default port (6969)
    iex> {:ok, pid} = Beetle.Server.start_link()

    # Or specify a custom port
    iex> {:ok, pid} = Beetle.Server.start_link(port: 8080)

  TCP server is started with the these specific options:

  - `:binary`         - Handles data in binary format rather than lists
  - `packet: :line`   - Processes incoming data line by line
  - `active: false`   - Socket starts in passive mode for initial setup
  - `reuseaddr: true` - Allows reusing the address if the server restarts

  Each client socket is configured with the following specific options:

  - `active: true`  - this automatically converts the incoming data to messages
                      and forwards it to the controlling process
  - `packet: :line` - tells the socket to handle data line by line
  - `buffer: 1024`  - sets the socket's receive buffer size to 1024 bytes
  """
  use GenServer

  require Logger

  # === Client 

  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  # === Server

  @impl true
  def init(opts) do
    port = Keyword.get(opts, :port, 6969)

    case :gen_tcp.listen(port, [:binary, active: false, reuseaddr: true, ip: {0, 0, 0, 0}]) do
      {:ok, listen_socket} ->
        GenServer.cast(self(), :accept)
        {:ok, listen_socket}

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
  def handle_cast(:accept, listen_socket) do
    with {:ok, client_socket} <- :gen_tcp.accept(listen_socket),
         {:ok, client_pid} <- Beetle.Server.ClientSupervisor.start_client(client_socket),
         :ok <- :inet.setopts(client_socket, active: true, buffer: 1024),
         :ok <- :gen_tcp.controlling_process(client_socket, client_pid) do
      GenServer.cast(self(), :accept)
      {:noreply, listen_socket}
    else
      {:error, reason} -> {:stop, reason, listen_socket}
    end
  end
end
