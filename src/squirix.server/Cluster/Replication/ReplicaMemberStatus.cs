using System;
using System.Runtime.InteropServices;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>The part of the replica status of a group member that the first start of a group reads.</summary>
/// <param name="MemberState">Whether the copy of the member holds the data of the group; <see langword="null" /> when the member did not report it.</param>
/// <param name="Ready">Whether the log of the copy is ready.</param>
/// <param name="TopologyFingerprint">The topology fingerprint the copy was created under.</param>
/// <param name="ConfigurationGeneration">The configuration generation of the copy.</param>
/// <param name="RefusalCode">The refusal code of the member; empty when it answered from its log.</param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct ReplicaMemberStatus(
    GroupMemberState? MemberState,
    bool Ready,
    ReadOnlyMemory<byte> TopologyFingerprint,
    ulong ConfigurationGeneration,
    string RefusalCode);
