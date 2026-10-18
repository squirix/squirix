using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>What the leader of a group did when it appended to its own group log, as the election driver reads it on every tick.</summary>
/// <param name="Appended">How many local appends succeeded since the committer was created.</param>
/// <param name="Failed">How many local appends failed, were refused, or ran out of their budget since the committer was created.</param>
/// <param name="Started">
/// How many local appends began since the committer was created. An append in flight at two ticks with the same value is one append that
/// does not return; a different value means the append in flight is a new one.
/// </param>
/// <param name="InFlight">Whether an append is in flight now. An append in flight is the sign of a flush that never returns.</param>
/// <param name="Refused">
/// Whether the log refused the last append that ended because it is not ready and no append succeeded since. It is the sign of a log that
/// latched its failure.
/// </param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct ReplicaAppendProgress(ulong Appended, ulong Failed, ulong Started, bool InFlight, bool Refused);
