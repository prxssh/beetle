defmodule Beetle.Storage.Bitcask.Keydir do
  @moduledoc """
  Keydir is an in-memory hash table that stores all the keys present in the
  Bitcask instance and maps it to an offset in the datafile where the log entry
  (value) resides.

  A single entry in the keydir has the following structure:

             -----------------------------------------------
    key --> | file_id | value_size | value_pos | timestamp |
            -----------------------------------------------

  - `file_id`: the ID of the datafile containing the value
  - `value_size`: size of the store value in bytes
  - `value_pos`: offset position in the datafile where the value resides
  - `timestamp`: unix-time at which the entry was written in the keydir
  """
end
