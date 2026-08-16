using Squirix.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>Outcome of a replica commit-index advance attempt.</summary>
/// <param name="Success">Determines whether the commit index was advanced.</param>
/// <param name="RefusalCode">Stable refusal marker when the commit index was not advanced; otherwise empty.</param>
/// <param name="CommitIndex">The durable commit index after processing the request.</param>
[Immutable]
internal readonly record struct FollowerLogCommitResult(bool Success, string RefusalCode, ulong CommitIndex);
