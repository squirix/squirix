using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Immutable expiration identity observed without lazy local deletion.</summary>
/// <param name="Version">Monotonic entry version.</param>
/// <param name="ExpiresUtc">Leader-computed absolute UTC expiration.</param>
[Immutable]
internal readonly record struct ReplicaExpirationCandidate(long Version, DateTime ExpiresUtc);
