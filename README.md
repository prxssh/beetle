# Beetle

Beetle is a [redis-compliant](https://redis.io), distributed, and persistent
database built using Elixir. It started as a fun project to learn about
distributed systems and databases, and is currently under heavy development.
Internally, it uses the Bitcask storage engine for faster data storage and
retrieval. Consensus is provided using Raft consensus algorithm.

## Features

- Support Redis Serialization Protocol Specification, making it extensible for
  use with any existing redis clients.
- Log-structured fast key-value store, capable of handling production grade
  traffic.
- Strong consistency guarantees with leader election and log replication.
- Supports transactions and command pipelining.
- Supports various data structures like strings, lists, hashes, bitmaps,
  bitfields, and pubsub.
- Built on Elixir for natural fault tolerance and distribution.

## Status

This is an experimental project under active development. While the core
functionality works, expect breaking changes and ongoing optimizations.

## Contributing

This is just a fun side project, so I'm not expecting any contributions.
However, if this project interests you, head over to the
[issues](https://github.com/prxssh/beetle/issues), and raise a pull request for
any of the open issues.

## Resources

- [Bitcask - A Log Structured Hash Table for Fast Key/Value Data](https://riak.com/assets/bitcask-intro.pdf)
- [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- [Redis Serialization Protocol Specification](https://redis.io/docs/latest/develop/reference/protocol-spec/) 
