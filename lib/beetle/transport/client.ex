defmodule Beetle.Transport.ClientSupervisor do
  @moduledoc """
  Dynamically supervises TCP client connection processes.

  Uses `DynamicSupervisor` to manage incoming TCP clients, creating new
  processes as client connects and cleaning them up when they disconnect.
  Implements a one-for-one supervisor strategy to isolate failures between
  client connections.
  """
  use DynamicSupervisor

  ########## Client

  def start_link(_), do: DynamicSupervisor.start_link(__MODULE__, nil, name: __MODULE__)

  @spec start_client(:gen_tcp.socket()) :: {:ok, pid()} | {:error, term()}
  def start_client(socket) do
    child_spec = {Beetle.Transport.Client, socket}
    DynamicSupervisor.start_child(__MODULE__, child_spec)
  end

  ########## Server

  @impl true
  def init(_), do: DynamicSupervisor.init(strategy: :one_for_one)
end

defmodule Beetle.Transport.Client do
  @moduledoc """
  Handles individual TCP client connections.

  Manages the communication with a single TCP client, processing incoming
  messages and sending messages. Each instance handles one client socket in
  active mode, receiving TCP messages as Erlang messages. 
  """
  use GenServer

  alias Beetle.Command

  require Logger

  defmodule State do
    defstruct socket: nil
  end

  ########## Client

  def start_link(socket), do: GenServer.start_link(__MODULE__, socket)

  ########## Server

  @impl true
  def init(socket) do
    :inet.setopts(socket, active: :once)

    {:ok, %State{socket: socket}}
  end

  @impl true
  def handle_info({:tcp, _, data}, state) do
    with {:ok, commands} <- Command.parse(data) |> IO.inspect(),
         {response, context} <- Command.execute(%{}, commands) |> IO.inspect(),
         :ok <- :gen_tcp.send(state.socket, response) do
      :inet.setopts(state.socket, active: :once)
      {:noreply, Map.merge(state, context)}
    else
      {:error, reason} ->
        Logger.error("#{__MODULE__} failed to handle client command: #{inspect(reason)}")

        {:stop, reason, state}
    end
  end

  @impl true
  def handle_info({:tcp_closed, _socket}, state) do
    Logger.debug("#{__MODULE__} client connection closed: #{inspect(state.socket)}")

    {:stop, :normal, state}
  end

  @impl true
  def handle_info({:tcp_error, _socket, reason}, state) do
    Logger.debug("#{__MODULE__} client connection error: #{inspect(state.socket)}")

    {:stop, reason, state}
  end
end
