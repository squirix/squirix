using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Fixed configuration for the deterministic election timer.</summary>
[Immutable]
internal sealed class ElectionTimerOptions
{
    /// <summary>Gets the one-shot election timeout armed while the node awaits a leader heartbeat.</summary>
    internal TimeSpan ElectionTimeout { get; init; } = TimeSpan.FromSeconds(1);
}
