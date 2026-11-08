# Related projects

This page surveys distributed caches and key-value stores that overlap with squirix, and how squirix differs from each.
Facts about other projects come from their public documentation and package pages as of October 2026; see
[Sources](#sources). They may have changed since.

## squirix in brief

- **Pure .NET.** Client SDK and server runtime are managed .NET 10 code with no native storage engine; the server runs as
  the `squirix-server` tool or embedded in an ASP.NET Core host. See [architecture.md](architecture.md).
- **Typed client.** Applications use `SquirixClient` and `ICache<T>` over HTTPS gRPC, with pluggable
  [serialization](serialization.md).
- **Replica groups with majority commit.** Each key has one owner group (static topology, consistent-hash routing).
  RF>=3 commits on a majority and can opt into automatic failover with quorum reads; RF=2 is a synchronous mirror.
  See [clustering.md](clustering.md) and [consistency.md](consistency.md).
- **CP under partitions.** A group without a majority refuses writes; with quorum reads a read that cannot be confirmed
  fails instead of returning a possibly stale value.
- **Durability.** RF=1 persistence flushes the node journal before applying a mutation to memory. Replicated writes use
  the replica group log as their durable source. See [persistence.md](persistence.md).
- **Protocol verification.** The replication protocol has an explicit-state model (`squirix.protocol-model`) with safety
  checks and conformance of production traces, run in CI.

Current limitations relevant to the comparison: no transactions across owners, static topology without automatic
rebalancing, and a 0.1 preview release.

## Overview

| Project                                                     | Server runtime | Replication                                             | Consistency of the main key-value path               |
|-------------------------------------------------------------|----------------|---------------------------------------------------------|------------------------------------------------------|
| squirix                                                     | .NET           | Majority commit per replica group, leader election      | CP; quorum reads with automatic failover (RF>=3)     |
| [Kahuna](#kahuna)                                           | .NET           | Raft group per partition                                | Strong; transactions with snapshot isolation         |
| [Slik.Cache](#slikcache)                                    | .NET           | Raft (dotNext)                                          | Leader-based; model not documented                   |
| [NCache](#ncache)                                           | .NET           | Synchronous or asynchronous replica updates             | Depends on topology and replication mode             |
| [Apache Ignite 3](#apache-ignite-3)                         | JVM            | Raft                                                    | ACID transactions, snapshot isolation                |
| [Hazelcast](#hazelcast)                                     | JVM            | Raft for the CP Subsystem; partition backups elsewhere  | CPMap linearizable; `IMap` and other structures AP   |
| [Garnet](#garnet)                                           | .NET           | Asynchronous leader-follower log shipping               | Async replicas can lag                               |
| [Redis Cluster](#redis-cluster-and-redisraft)               | C              | Asynchronous                                            | Not CP; can lose acknowledged writes                 |
| [RedisRaft](#redis-cluster-and-redisraft)                   | C (module)     | Raft                                                    | Strong; not production-ready per its README          |
| [etcd, Consul, ZooKeeper](#etcd-consul-and-zookeeper)       | Go / Java      | Raft / ZAB                                              | Read guarantees depend on API and mode               |

## .NET projects

### Kahuna

Kahuna is a .NET 10 server cluster that combines distributed locks with fencing tokens and leases, a key-value store with
transactions and watchers, and an ordered ID sequencer. Each partition runs its own Raft group on the project's
Kommander library; transactions use two-phase commit with MVCC and snapshot isolation. The project reports Jepsen tests
for serializability, linearizability and durability under partitions, crashes and clock skew; the suite lives in a
separate repository.

How squirix differs: Kahuna targets coordination and application state (locks, sequences, multi-key transactions).
squirix is a cache: typed `ICache<T>` access with serialization, memory-pressure admission, expiration, and journal plus
snapshot persistence, without multi-key transactions.

### Slik.Cache

Slik.Cache is a distributed in-process cache that implements `IDistributedCache` on top of the Raft cluster from the
dotNext libraries. Writes are redirected to the leader and replicated to every node. It targets .NET 5 and 6; log
compaction and a binary log format are listed as open roadmap items.

How squirix differs: squirix is a client/server cluster with partitioned replica groups rather than a full copy on every
node, ships its own journal and snapshot formats with compaction, and documents its consistency guarantees.

### dotNext Raft (DotNext.Net.Cluster)

Part of the .NET Foundation dotNext libraries: a Raft implementation with a persistent write-ahead log, replication and
leader election. It is a framework for building clustered services, not a cache or key-value product; Slik.Cache is built
on it.

### NCache

NCache is a commercial distributed cache written in .NET with an open source edition. Its Partition-Replica topology
supports synchronous replication, where the write waits for its replica before returning, and asynchronous replication,
where replication is queued in the background. The asynchronous mode can lose queued writes if the active node fails
before they are replicated. See the topology documentation in [Sources](#sources).

How squirix differs: Squirix uses majority commit per replica group, with opt-in leader election and quorum reads for
RF>=3. NCache offers different replication modes by topology; compare the selected mode and its documented failure
behavior rather than inferring its guarantees from the word synchronous.

### Garnet

Garnet is Microsoft's .NET cache server compatible with the Redis protocol. Its cluster mode replicates through
asynchronous leader-follower log shipping, with options that trade performance for durability; chained replication is
not supported.

How squirix differs: Garnet optimizes for throughput and Redis compatibility with asynchronous replicas; squirix exposes
its own typed gRPC API and chooses consistency over availability for replicated groups.

## JVM data grids

### Apache Ignite 3

Ignite 3 is a distributed database whose transactions commit through Raft, with ACID semantics, MVCC and snapshot
isolation, plus SQL and compute. It provides a full .NET client (`Apache.Ignite` on NuGet) with key-value, SQL,
transactions and a LINQ provider; the server nodes run on the JVM.

How squirix differs: Ignite is a general-purpose database with SQL and cross-partition transactions; squirix is a
narrower cache with a .NET server, so a .NET team operates one runtime end to end.

### Hazelcast

Hazelcast runs on the JVM. With CP members configured, its CP Subsystem uses Raft for CPMap, locks, atomics,
semaphores and latches; other structures, including the regular map, are AP. The referenced Hazelcast 5.5 documentation
marks the CP Subsystem as an Enterprise feature and distinguishes it from the weaker default unsafe mode. Its CP
guarantees should not be assumed for an unconfigured cluster.

How squirix differs: in squirix the main cache path is the consistent one, rather than a separate CP structure next to
an AP map.

## Other key-value systems

### Redis Cluster and RedisRaft

Redis Cluster uses asynchronous replication and documents cases in which acknowledged writes can be lost in its
[cluster specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/).
RedisRaft is a Raft-based module for strongly consistent Redis; its README says it is not ready for production use.

### etcd, Consul and ZooKeeper

These systems target coordination and metadata use cases such as configuration and service discovery. Their read
guarantees differ; using consensus for writes does not make every read API linearizable:

- etcd provides linearizable key-value operations by default; its optional serializable reads can return stale data.
  See [etcd API guarantees](https://etcd.io/docs/v3.6/learning/api_guarantees/).
- Consul supports `stale`, `default`, and `consistent` HTTP read modes. The `consistent` mode confirms leadership with
  a quorum; the other modes have weaker guarantees. See
  [Consul consistency modes](https://developer.hashicorp.com/consul/api-docs/features/consistency).
- ZooKeeper has linearizable writes, while ordinary reads are sequentially consistent and may be stale. See
  [ZooKeeper consistency guarantees](https://zookeeper.apache.org/doc/current/zookeeperInternals#sc_consistency).

## Sources

- Kahuna: [Kahuna.Server on NuGet](https://www.nuget.org/packages/Kahuna.Server), [kahunakv/kahuna](https://github.com/kahunakv/kahuna),
  [Kommander on NuGet](https://www.nuget.org/packages/Kommander)
- Slik.Cache: [Insvald/Slik](https://github.com/Insvald/Slik)
- dotNext: [.NET Foundation project page](https://dotnetfoundation.org/projects/dotnext)
- NCache: [product page](https://www.alachisoft.com/ncache/), [partition-replica replication modes](https://www.alachisoft.com/resources/docs/ncache/admin-guide/partitioned-topologies.html)
- Garnet: [cluster replication](https://microsoft.github.io/garnet/docs/cluster/replication)
- Apache Ignite: [transactions](https://ignite.apache.org/features/transactions.html), [Apache.Ignite on NuGet](https://www.nuget.org/packages/apache.ignite)
- Hazelcast: [CP Subsystem](https://docs.hazelcast.com/hazelcast/5.5/cp-subsystem/cp-subsystem)
- Redis: [Jepsen analyses](https://jepsen.io/analyses), [jepsen-io/redis](https://clojars.org/io.jepsen/redis), [redislabs/redisraft](https://github.com/redislabs/redisraft)
