defmodule Beetle.Transport.ClientSupervisor do
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
    child_spec = {Beetle.Transport.Client, socket}
    DynamicSupervisor.start_child(__MODULE__, child_spec)
  end

  # === Server

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

  require Logger

  alias Beetle.Command.Types.Transaction
  alias Beetle.Command
  alias Beetle.Transaction

  defmodule State do
    defstruct socket: nil, transaction_manager: Transaction.new()
  end

  ########### Client

  def start_link(socket), do: GenServer.start_link(__MODULE__, socket)

  ########### Server Callbaks

  @impl true
  def init(socket) do
    :inet.setopts(socket, active: :once)
    {:ok, %State{socket: socket}}
  end

  @impl true
  def handle_info({:tcp, _, data}, state) do
    with {:ok, commands} <- Command.parse(data),
         {response, updated_transaction_context} <-
           Command.execute(commands, state.transaction_manager),
         :ok <- :gen_tcp.send(state.socket, response) do
      :inet.setopts(state.socket, active: :once)
      {:noreply, %State{state | transaction_manager: updated_transaction_context}}
    else
      {:error, :command_parse, reason} ->
        :gen_tcp.send(state.socket, reason)
        :inet.setopts(state.socket, active: :once)
        {:noreply, %State{state | transaction_manager: Transaction.new()}}

      {:error, reason} ->
        {:stop, reason, %State{state | transaction_manager: Transaction.new()}}
    end
  end

  @impl true
  def handle_info({:tcp_closed, _socket}, state), do: {:stop, :normal, state}

  @impl true
  def handle_info({:tcp_error, _socket, reason}, state), do: {:stop, reason, state}
end
