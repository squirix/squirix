using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>How far a led group has applied its entries, and how far a majority already holds them.</summary>
/// <param name="AppliedIndex">The highest log index applied to memory, never above <paramref name="MajorityIndex" />.</param>
/// <param name="MajorityIndex">
/// The highest log index a majority backs: the commit index, or a higher index the written majority covers and the commit rule of the
/// recovered tail lets the leader commit. Entries without a majority are not counted.
/// </param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct ReplicaApplyProgress(ulong AppliedIndex, ulong MajorityIndex)
{
    /// <summary>Gets a value indicating whether an entry a majority backs is not applied yet.</summary>
    internal bool IsBehind => AppliedIndex < MajorityIndex;
}
