defmodule Beetle.Command.Registry do
  @moduledoc """
  Command registry that routes commands to appropriate handlers
  """

  alias Beetle.Command.Engine
  alias Beetle.Command.DataTypes

  @data_type_string :string

  @command_modules %{
    @data_type_string => DataTypes.String
  }

  @command_types %{
    "GET" => @data_type_string,
    "SET" => @data_type_string
  }

  @spec handle([Engine.t()]) :: {:ok, any()} | {:error, String.t()}
  def handle(commands) do
    @command_types
    |> Map.get(command)
    |> case do
      nil ->
        invalid_command(command, args)

      type ->
        module = Map.fetch!(@command_modules, type)
        module.handle(command, args)
    end
  end

  @spec invalid_command(String.t(), [String.t()]) :: {:error, String.t()}
  defp invalid_command(command, args) do
    args_str = args |> Enum.map_join(" ", &"'#{&1}'")
    {:error, "ERR unkown command '#{command}', with args beginning with: #{args_str}"}
  end
end
