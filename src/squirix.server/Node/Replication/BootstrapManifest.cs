using System;
using System.Collections.Generic;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Replication;

/// <summary>Versioned durable intent and resumable progress for one RF=1 to RF&gt;1 bootstrap.</summary>
[Immutable]
internal sealed class BootstrapManifest
{
    /// <summary>Gets the manifest format version.</summary>
    internal ushort FormatVersion { get; init; } = 1;

    /// <summary>Gets per-group resumable progress in deterministic order.</summary>
    internal required IReadOnlyList<BootstrapGroupProgress> Groups { get; init; }

    /// <summary>Gets the source cluster identity.</summary>
    internal required string SourceClusterId { get; init; }

    /// <summary>Gets the source RF=1 topology fingerprint.</summary>
    internal required ReadOnlyMemory<byte> SourceFingerprint { get; init; }

    /// <summary>Gets the source configuration generation.</summary>
    internal ulong SourceGeneration { get; init; }

    /// <summary>Gets the target RF&gt;1 topology fingerprint.</summary>
    internal required ReadOnlyMemory<byte> TargetFingerprint { get; init; }

    /// <summary>Gets the target configuration generation.</summary>
    internal ulong TargetGeneration { get; init; }

    /// <summary>Gets the target replica factor.</summary>
    internal int TargetReplicaCount { get; init; }
}
