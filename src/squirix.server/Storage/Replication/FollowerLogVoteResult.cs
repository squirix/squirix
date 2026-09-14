using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>Outcome of a ballot or pre-vote evaluation for one replica group.</summary>
/// <param name="Granted">Determines whether the vote was granted (or would be granted for a pre-vote probe).</param>
/// <param name="RefusalCode">Stable refusal marker when the vote was not granted; otherwise empty.</param>
/// <param name="CurrentTerm">The durable term after processing the request; unchanged by pre-vote probes.</param>
[Immutable]
internal readonly record struct FollowerLogVoteResult(bool Granted, string RefusalCode, ulong CurrentTerm);
