namespace Squirix.Server.Storage.Replication;

/// <summary>A durable applied boundary and its WAL coordinates, captured under the log gate.</summary>
/// <param name="Snapshot">The captured snapshot.</param>
/// <param name="PreviousIndex">The fallback snapshot boundary.</param>
/// <param name="RetainedOffset">The offset where the retained WAL starts.</param>
/// <param name="CurrentTerm">The local term when the boundary was captured.</param>
[System.Runtime.InteropServices.StructLayout(System.Runtime.InteropServices.LayoutKind.Auto)]
internal readonly record struct GroupSnapshotCompaction(GroupSnapshot Snapshot, ulong PreviousIndex, long RetainedOffset, ulong CurrentTerm);
