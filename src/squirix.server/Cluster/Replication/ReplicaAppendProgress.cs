using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>What the leader of a group did when it appended to its own group log, as the election driver reads it on every tick.</summary>
/// <param name="Appended">How many local appends succeeded since the committer was created.</param>
/// <param name="Failed">How many local appends failed, were refused, or ran out of their budget since the committer was created.</param>
/// <param name="Blocked">
/// Whether an append is in flight now, or the last append that ended was refused because the log is not ready and no append succeeded
/// since. An in-flight append is the sign of a flush that never returns; the refusal is the sign of a log that latched its failure.
/// </param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct ReplicaAppendProgress(ulong Appended, ulong Failed, bool Blocked);
