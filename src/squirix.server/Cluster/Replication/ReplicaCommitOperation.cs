using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>A registered commit operation: the shared attempt, the resolution of its execution, and the outcome replayed for an entry of a run.</summary>
/// <param name="Attempt">The attempt shared by the operations of one execution.</param>
/// <param name="Resolution">The resolution of the execution.</param>
/// <param name="Outcome">The outcome of this entry of a run, or <see langword="null" /> for a single entry, whose outcome is the resolution.</param>
[Immutable]
internal sealed record ReplicaCommitOperation(ReplicaCommitAttempt Attempt, Task<ReadOnlyMemory<byte>> Resolution, ReadOnlyMemory<byte>? Outcome = null);
