using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>One cache entry of a state transfer, in the form the leader encodes it and the wire carries it.</summary>
/// <param name="CacheName">The cache the key belongs to.</param>
/// <param name="Key">The UTF-8 bytes of the key.</param>
/// <param name="Payload">The durable entry encoding, which keeps the version and the tags; the deadline travels apart from it.</param>
/// <param name="ExpiresUtcTicks">The absolute deadline in UTC ticks; zero for an entry without one.</param>
[Immutable]
internal readonly record struct ReplicaStateEntry(string CacheName, ReadOnlyMemory<byte> Key, ReadOnlyMemory<byte> Payload, long ExpiresUtcTicks);
