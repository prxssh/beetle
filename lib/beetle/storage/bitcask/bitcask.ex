defmodule Beetle.Storage.Bitcask do
  @moduledoc """
  Bitcask is a log-structured key-value store designed to handle
  production-grade traffic. 

  It uses a lot of principles from log-structured file systems and draws
  inspiration from a number of designs that involve log file merging. It
  essentially is just a directory of append-only files with a fixed structure
  and an in-memory index holding the keys mapped to a bunch of information
  necessary for point lookups.
  """
end
