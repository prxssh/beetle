defmodule Beetle.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    path = maybe_extract_path(System.argv())

    children = [
      {Task.Supervisor, name: Beetle.TaskSupervisor},
      {Beetle.Config, path},
      {Beetle.Storage.Supervisor, []},
      {Beetle.Transport.Server, []},
      {Beetle.Transport.ClientSupervisor, []}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Beetle.Supervisor)
  end

  defp maybe_extract_path([]), do: nil
  defp maybe_extract_path(["test"]), do: nil
  defp maybe_extract_path([_path, location]), do: Path.expand(location)
end
