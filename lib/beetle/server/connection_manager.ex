defmodule Beetle.Server.ConnectionManager do
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
  alias Beetle.Config.State, as: Config

  @name __MODULE__
  @client_supervisor Beetle.DynamicSupervisor

  # === Client

  def start_link(_), do: GenServer.start_link(@name, nil, name: @name)

  # === Server

  @impl true
  def init(_) do
    {host, port} = {Config.get_host(), Config.get_port()}
    dbg(host)
    dbg(port)
    state = %{socket: %{}, clients: [], opts: %{host: host, port: port}}

    {:ok, state, {:continue, :start_tcp_server}}
  end

  @impl true
  def handle_continue(:start_tcp_server, state) do
    state.opts.port
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
