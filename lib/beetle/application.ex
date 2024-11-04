defmodule Beetle.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {DynamicSupervisor, strategy: :one_for_one, name: Beetle.ClientSupervisor},
      {Beetle.Server.ConnectionManager, []}
    ]

    opts = [strategy: :one_for_one, name: Beetle.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
