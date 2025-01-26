# Beetle

Beetle is a Elixir implementation of [Bitcask by Riak](https://riak.com/assets/bitcask-intro.pdf) 
paper and aims to closely follow the spec.

Bitcask is one of the most efficient embedded key-value database designed to
handle production-grade traffic. It uses a log-structured hash table for fast
key-value data which, in a simple language, means that the data will be written
sequentially to an append-only log file and there will be pointers for each
`key` pointing to the `position` of its log entry.

## Benefits of this approach

- Low latency for read and write operations
- High Write Throughput
- Single disk seek to retrieve any value
- Predictable lookup and insert performance
- Crash recovery is fast and bounded
- Backing up is easy - Just copying the directory would suffice

## Limitations

The main limitation is that all the keys must fit in RAM since they're held
inside das an in-memory hash table. This adds a huge constraint on the system
that it needs to have enough memory to contain the entire keyspace along with
other essentials like Filesystem buffers. Although this weakness seems a major
one but the solution to this is fairly simple. We can typically shard the keys
and scale it  horizontally without losing much of the basic operations like
Create, Read, Update, and Delete.

## Roadmap and Status

The high-level ambitious plan for the project, in order:

|  #  | Step                                                      | Status |
| :-: | --------------------------------------------------------- | :----: |
|  1  | Bitcask Implementation                                    |   ✅   |
|  2  | Redis Serialization Protocol                              |   ✅   |
|  3  | Basic commands like GET, SET, DEL                         |   ✅   |
|  4  | Redis Transactions & Command Pipelining                   |   ✅   |
|  5  | Support database sharding                                 |   ✅   |
|  6  | Support strings datatype                                  |   ✅   |
|  7  | Support list datatype                                     |   ❌   |
|  8  | Support hash datatype                                     |   ❌   |
|  9  | Support bloom filter datatype                             |   ❌   |
|  10 | Support bitmap datatype                                   |   ❌   |
|  11 | Support pubsub                                            |   ❌   |
|  12 | Test cases for the modules                                |   ❌   |
|  13 | Make it distributed using Raft consensus algorithm        |   ❌   |

## Get started

### Setting up Beetle with Docker

The easiest way to get started with Beetle is using [Docker](https://www.docker.com/) by running the following command:


```bash
docker build -t beetle .

docker run -p 6969:6969 --name beetle beetle
```

The above commands will build the docker image and start Beetle server running
locally on the port `6969` and you can connect to it using 
[Redis CLI](https://redis.io/docs/latest/develop/tools/cli/)

### Running the application locally

To run Beetle locally either for development or just playing around, you need
to have the [Elixir](https://elixir-lang.org/) installed.

```bash
git clone https://github.com/prxssh/beetle
cd beetle
iex -S mix run
```

This will start the Beetle server on port `6969`. 

Additonally you can also run Beetle by providing a configuration file:

```bash
iex -S mix run -- config example/beetle.conf
```

## Benchmarks 

Performance benchmarks on M4 Macbook Pro using [Redis benchmark](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/benchmarks/)
tool with 100K operations across 10 million unique keys:

### Get Operations

```bash
$ redis-benchmark -h localhost -p 6969 -n 100000 -r 10000000 -c 100 -t get

Summary:
  throughput summary: 136798.91 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.433     0.136     0.407     0.519     1.271     1.751
```

### Set Operations

Tested with 100 concurrent clients

```bash
$ redis-benchmark -h localhost -p 6969 -n 100000 -r 10000000 -c 100 -t set

Summary:
  throughput summary: 137362.64 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.428     0.128     0.407     0.487     1.255     1.559
```

### Performance Summary

- Sustained throughput of 136K+ operations per second
- Sub-millisecond average latency for both read and write operations
- P99 latency under 1.3ms for reads and writes
- Consistent performance across large keyspace (10M unique keys)

## References

- [Bitcask paper](https://riak.com/assets/bitcask-intro.pdf)
- [Redis Serialization Protocol Specification](https://redis.io/docs/latest/develop/reference/protocol-spec/)
