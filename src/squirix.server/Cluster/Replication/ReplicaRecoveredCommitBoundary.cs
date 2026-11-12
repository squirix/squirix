using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Current-term commit limits without references to recovered payloads.</summary>
/// <param name="LastIndex">Last index of the recovered tail.</param>
/// <param name="FirstCurrentTermIndex">First recovered index from the current term, or null when absent.</param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct ReplicaRecoveredCommitBoundary(ulong LastIndex, ulong? FirstCurrentTermIndex)
{
    /// <summary>Determines whether a majority-backed index passes the recovered-tail current-term rule.</summary>
    /// <param name="index">Commit candidate index.</param>
    /// <returns>Whether the index reaches the current-term boundary or exceeds the recovered tail.</returns>
    internal bool CanCommitThrough(ulong index) => index > LastIndex || (FirstCurrentTermIndex is { } first && index >= first);
}
