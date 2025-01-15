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
|  4  | Data types - strings, lists, hashes, bitmaps, bitfields   |   ❌   |
|  5  | Make it distributed using Raft Consensus Algorithm        |   ❌   |
|  7  | Support 100K+ read/write operations per second            |   ⚠️    |
|  N  | Fancy features (to be expanded upon later)                |   ❌   |

## Benchmarks 

Performance benchmarks using [Redis benchmark](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/benchmarks/)
tool with 100K+ operations across 10 million unique keys:

### Get Operations

```bash
$ redis-benchmark -h localhost -p 6969 -n 100000 -r 10000000 -t get

Summary:
  throughput summary: 85324.23 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.470     0.096     0.439     0.799     0.975     9.247
```

### Set Operations

Tested with 100 concurrent clients

```bash
$ redis-benchmark -h localhost -p 6969 -n 100000 -r 10000000 -c 100 -t set

Summary:
  throughput summary: 92165.90 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.821     0.144     0.735     1.111     6.167     7.423
```

### Performance Summary

- Sustained throughput of 85K-92K operations per second
- Sub-millisecond average latency for both read and write operations
- P99 latency under 1ms for reads and under 7ms for writes
- Consistent performance across large keyspace (10M unique keys)


## References

- [Bitcask paper](https://riak.com/assets/bitcask-intro.pdf)
- [Redis Serialization Protocol Specification](https://redis.io/docs/latest/develop/reference/protocol-spec/)
