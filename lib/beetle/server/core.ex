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

  alias Beetle.Config
  alias Beetle.Server.TCP

  @name __MODULE__
  @client_supervisor Beetle.ClientSupervisor

  # === Client

  def start_link(config_path), do: GenServer.start_link(@name, config_path, name: @name)

  # === Server

  @impl true
  def init(config_path) do
    config_path
    |> Config.load()
    |> case do
      {:ok, config} ->
        state = %{socket: %{}, clients: [], config: config}
        {:ok, state, {:continue, :start_tcp_server}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_continue(:start_tcp_server, state) do
    state.config.port
    |> TCP.listen()
    |> case do
      {:ok, socket} ->
        Logger.info("#{__MODULE__}: started tcp server on 6969")

        {:noreply, %{state | socket: socket}, {:continue, :accept}}

      {:error, reason} ->
        Logger.error("#{__MODULE__}: failed to start tcp server: #{inspect(reason)}")

        {:stop, reason, state}
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
  alias Beetle.Parser.Core, as: Parser

  @client_handler __MODULE__

  # === Client

  def child_spec(opts) do
    %{
      id: @client_handler,
      restart: :transient,
      start: {@client_handler, :start_link, [opts]}
    }
  end

  def start_link(socket: client_socket),
    do: GenServer.start_link(@client_handler, client_socket)

  # === Server

  @impl true
  def init(client_socket), do: {:ok, client_socket}

  @impl true
  def handle_info({:tcp, socket, packet}, socket) do
    with {:ok, parsed_command} <- Parser.decode(packet),
         :ok <- TCP.write(parsed_command, socket) do
      {:noreply, socket}
    else
      {:error, reason} ->
        Logger.error("failed to handle incoming tcp packet #{inspect(reason)}")
        {:stop, :normal, socket}
    end
  end

  @impl true
  def handle_info({:tcp_closed, socket}, socket) do
    Logger.notice("tcp closed: terminating connection")
    {:stop, :normal, socket}
  end

  @impl true
  def handle_info({:tcp_error, socket, reason}, socket) do
    Logger.notice("tcp error: #{inspect(reason)}")
    {:stop, :normal, socket}
  end

  @impl true
  def terminate(_reason, socket), do: TCP.close(socket)
end
