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
- **Durability.** Journal plus snapshots; with persistence on, a read never returns a value whose journal frame is not
  durable. See [persistence.md](persistence.md).
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
| [Garnet](#garnet)                                           | .NET           | Asynchronous leader-follower log shipping               | Not CP                                               |
| [Redis Cluster](#redis-cluster-and-redisraft)               | C              | Asynchronous                                            | Not CP; can lose acknowledged writes                 |
| [RedisRaft](#redis-cluster-and-redisraft)                   | C (module)     | Raft                                                    | Strong; not production-ready per its README          |
| [etcd, Consul, ZooKeeper](#etcd-consul-and-zookeeper)       | Go / Java      | Raft / ZAB                                              | Linearizable; built for coordination metadata        |

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

NCache is a commercial distributed cache written in .NET with an open source edition. Its mirror and partition-replica
topologies can replicate synchronously, where a write waits for every replica, or asynchronously, where replicas are
updated after the primary and may briefly diverge.

How squirix differs: synchronous replication to all replicas is not majority consensus. squirix commits on a majority,
elects a new leader on failure (RF>=3), and with quorum reads confirms leadership before each read, so a minority cannot
serve stale or diverging values.

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

Hazelcast runs on the JVM. Its CP Subsystem uses Raft and keeps CPMap, locks, atomics, semaphores and latches
linearizable; other structures, including the regular map, are AP.

How squirix differs: in squirix the main cache path is the consistent one, rather than a separate CP structure next to
an AP map.

## Other key-value systems

### Redis Cluster and RedisRaft

Redis Cluster replicates asynchronously and can lose acknowledged writes by design, as Jepsen analyses showed.
RedisRaft is a Raft-based module for strongly consistent Redis; its README says it is not ready for production use.

### etcd, Consul and ZooKeeper

Linearizable stores built on consensus, designed for small amounts of coordination metadata (configuration, service
discovery, leader election) rather than as high-volume application caches.

## Sources

- Kahuna: [Kahuna.Server on NuGet](https://www.nuget.org/packages/Kahuna.Server), [kahunakv/kahuna](https://github.com/kahunakv/kahuna),
  [Kommander on NuGet](https://www.nuget.org/packages/Kommander)
- Slik.Cache: [Insvald/Slik](https://github.com/Insvald/Slik)
- dotNext: [.NET Foundation project page](https://dotnetfoundation.org/projects/dotnext)
- NCache: [product page](https://www.alachisoft.com/ncache/), [consistency and data replication](https://alachisoft.com/blogs/ncache-essentials-consistency-and-data-replication/)
- Garnet: [cluster replication](https://microsoft.github.io/garnet/docs/cluster/replication)
- Apache Ignite: [transactions](https://ignite.apache.org/features/transactions.html), [Apache.Ignite on NuGet](https://www.nuget.org/packages/apache.ignite)
- Hazelcast: [CP Subsystem](https://docs.hazelcast.com/hazelcast/5.5/cp-subsystem/cp-subsystem)
- Redis: [Jepsen analyses](https://jepsen.io/analyses), [jepsen-io/redis](https://clojars.org/io.jepsen/redis), [redislabs/redisraft](https://github.com/redislabs/redisraft)
