defmodule Beetle.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    config_path = System.fetch_env!("BEETLE_CONFIG")

    children = [
      {DynamicSupervisor, strategy: :one_for_one, name: Beetle.DynamicSupervisor},
      {Beetle.Config.State, config_path},
      {Beetle.Server.ConnectionManager, []},
      {Beetle.Storage.Engine, []}
    ]

    opts = [strategy: :one_for_one, name: Beetle.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
