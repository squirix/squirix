using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Outcome of one leader-side Log Matching probe against a follower.</summary>
/// <param name="Kind">Probe verdict.</param>
/// <param name="LastLogIndex">Follower last log index reported with the verdict; zero when none was reported.</param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct ReplicaProbeResult(ReplicaProbeKind Kind, ulong LastLogIndex)
{
    /// <summary>Gets a value indicating whether the follower answered the probe from its copy, which makes it a member that is reachable.</summary>
    internal bool Answered => Kind is ReplicaProbeKind.Accepted or ReplicaProbeKind.LogMismatch or ReplicaProbeKind.Incomplete;
}
