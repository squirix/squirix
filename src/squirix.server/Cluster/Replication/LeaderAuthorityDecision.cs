using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Outcome of a leader authority check.</summary>
/// <param name="Allowed">Whether the request may be served.</param>
/// <param name="Denial">The denial reason when <paramref name="Allowed" /> is <see langword="false" />.</param>
[Immutable]
internal readonly record struct LeaderAuthorityDecision(bool Allowed, LeaderAuthorityDenial Denial);
