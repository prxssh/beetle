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
    child_spec = {Beetle.Server.Client, socket}
    DynamicSupervisor.start_child(__MODULE__, child_spec)
  end

  # === Server

  @impl true
  def init(_), do: DynamicSupervisor.init(strategy: :one_for_one)
end

defmodule Beetle.Server.Client do
  @moduledoc """
  Handles individual TCP client connections.

  Manages the communication with a single TCP client, processing incoming
  messages and sending messages. Each instance handles one client socket in
  active mode, receiving TCP messages as Erlang messages. 
  """
  use GenServer

  require Logger

  alias Beetle.Command
  alias Beetle.Protocol.Encoder

  defmodule State do
    defstruct socket: nil,
              command_queue: [],
              in_transaction: false
  end

  # === Client

  def start_link(socket), do: GenServer.start_link(__MODULE__, socket)

  # === Server

  @impl true
  def init(socket), do: {:ok, %State{socket: socket}}

  @impl true
  def handle_info({:tcp, _, data}, state) do
    {response, updated_state} =
      data
      |> Command.parse()
      |> process_commands(state)

    dbg(updated_state)
    :gen_tcp.send(state.socket, response)

    {:noreply, updated_state}
  end

  @impl true
  def handle_info({:tcp_closed, _socket}, state), do: {:stop, :normal, state}

  @impl true
  def handle_info({:tcp_error, _socket, reason}, state) do
    Logger.error("TCP error: #{inspect(reason)}")
    {:stop, reason, state}
  end

  # === Private

  defp process_commands({:ok, commands}, state) when length(commands) == 1 do
    commands
    |> List.first()
    |> case do
      %Command{command: "MULTI"} ->
        transaction_start(state)

      %Command{command: "DISCARD"} ->
        transaction_discard(state)

      %Command{command: "EXEC"} ->
        transaction_execute(state)

      command ->
        if state.in_transaction,
          do: transaction_enqueue_command(command, state),
          else: {Command.execute(command), state}
    end
  end

  defp process_commands({:ok, commands}, _), do: Command.execute(commands)

  defp process_commands(error, _), do: Encoder.encode(error)

  defp transaction_start(state) do
    if state.in_transaction do
      response = Encoder.encode({:error, "ERR mutli calls can not be nested"})
      {response, %State{socket: state.socket}}
    else
      {Encoder.encode("OK"), %State{state | in_transaction: true}}
    end
  end

  defp transaction_discard(state),
    do: {Encoder.encode("OK"), %State{socket: state.socket}}

  defp transaction_execute(state) do
    if state.in_transaction do
      commands = Enum.reverse(state.command_queue)
      result = Command.execute_transaction(commands)

      {result, %State{socket: state.socket}}
    else
      {Encoder.encode({:error, "ERR EXEC without MULTI"}), %State{socket: state.socket}}
    end
  end

  defp transaction_enqueue_command(command, state) do
    {Encoder.encode("QUEUED"), %State{state | command_queue: [command | state.command_queue]}}
  end
end
