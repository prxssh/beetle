defmodule Beetle.Server.TCP do
  @moduledoc """
  A TCP server implementation providing a clean interface over Erlang's
  `:gen_tcp`.

  This module abstracts common TCP server operations into a simpler interface,
  handling the complexity of socket operations while maintaining all the
  robustness of the underlying `:gen_tcp` implementation.

  ## Socket Options

  The default socket options are:
  - `packet: :line`: receives data line by line
  - `:binary`: receive data as binaries instead of lists
  - `backlog: 1024`: the default size for connection backlogs
  - `active: false`: blocks on `:gen_tcp.recv/2` until data is available
  - `reuseaddr: true`: allows reuse of local addresses in case of listener
     crashes
  """

  alias :gen_tcp, as: GenTCP

  @default_listen_opts [:binary, reuseaddr: true, active: false, backlog: 1024, packet: :line]

  @doc """
  Starts a TCP server listening on the specified port and with the provided
  options or using default options defined above.
  """
  @spec listen(pos_integer(), Keyword.t()) :: {:ok, port()} | {:error, any()}
  def listen(port, opts \\ @default_listen_opts) do
    port
    |> GenTCP.listen(opts)
    |> case do
      {:ok, socket} -> {:ok, socket}
      error -> error
    end
  end

  @doc """
  Accepts a client connection on the given socket.

  Blocks until a client connects or error occurs.
  """
  @spec accept(term()) :: {:ok, port()} | {:error, any()}
  def accept(socket) do
    socket
    |> GenTCP.accept()
    |> case do
      {:ok, client_socket} -> {:ok, client_socket}
      error -> error
    end
  end

  @doc """
  Reads data from a client socket. 

  In passive mode (`active: false`), blocks until data is available or the
  socket is closed. In active mode, data is delivered as messages to the
  process.
  """
  @spec read(port()) :: {:ok, String.t() | binary() | term()} | {:error, any()}
  def read(socket) do
    socket
    |> GenTCP.recv(0)
    |> case do
      {:ok, data} -> {:ok, data}
      error -> error
    end
  end

  @doc """
  Writes data to the client socket.
  """
  @spec write(binary(), port()) :: :ok | {:error, any()}
  def write(packet, socket), do: GenTCP.send(socket, packet)

  @doc """
  Closes a socket.

  This will close a listen socket or a client socket.
  """
  @spec close(port()) :: :ok
  def close(socket), do: GenTCP.close(socket)

  @doc """
  Transfers the control of a socket to another process and sets it to active
  mode.

  In Erlang/Elixir, each socket has a controlling process which receives all
  messages from that socket. By default, this is the process that created the
  socket. When building a TCP server, we often need to transfer control to
  another process, typically a client handler.

  After transferring control, the socket is set to active mode, which means: 
  - Messages from the socket are automatically delivered to the controlling
  process.
  - Messages come in the format: 
    * `{:tcp, socket, data}`: for received data
    * `{:tcp_closed, socket}`: when client closes the connection
    * `{:tcp_error, socket, reason}`: when error occurs
  """
  @spec set_controlling_process(port(), pid()) :: :ok | {:error, any()}
  def set_controlling_process(socket, pid) do
    with :ok <- GenTCP.controlling_process(socket, pid),
         :ok <- :inet.setopts(socket, active: true) do
      :ok
    else
      error -> error
    end
  end
end
