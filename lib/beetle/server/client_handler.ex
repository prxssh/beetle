defmodule Beetle.Server.ClientHandler do
  @moduledoc """
  GenServer implementation for handling individual TCP client connections.
  """
  use GenServer

  require Logger

  alias Beetle.Server.TCP
  alias Beetle.Command.Engine

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
    with {:ok, parsed_command} <- Engine.execute(packet),
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
