using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster;

/// <summary>What this node knows of the leader of one replica group, read in one consistent view.</summary>
/// <param name="Served">Whether this node holds election state for the group; <see langword="default" /> views are unserved.</param>
/// <param name="HasAuthority">Whether this node leads the group and its leader-term entry is committed.</param>
/// <param name="LedWithoutAuthority">Whether this node leads the group without authority: its promotion is pending or a higher term deposed it.</param>
/// <param name="Term">The term this node acts in: the term it follows, campaigns in, or leads.</param>
/// <param name="HighestObservedTerm">The highest term this node saw for the group.</param>
/// <param name="Known">The leader a request for the group goes to; <see langword="default" /> when none is known.</param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct GroupLeaderView(
    bool Served,
    bool HasAuthority,
    bool LedWithoutAuthority,
    ulong Term,
    ulong HighestObservedTerm,
    LeaderRoute Known)
{
    /// <summary>Gets a value indicating whether a leader is known for the group.</summary>
    internal bool HasLeader => !string.IsNullOrEmpty(Known.NodeId);
}
