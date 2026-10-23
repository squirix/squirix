using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>The answer of a group member to the offer of the owner to take part in the first start of the group.</summary>
/// <param name="Accepted">Whether the copy is unseeded and now holds the offer durably.</param>
/// <param name="MemberState">The state of the copy after the call; <see langword="null" /> when the member does not serve the group or did not report it.</param>
/// <param name="RefusalCode">The refusal code; empty when the offer was accepted.</param>
[Immutable]
internal readonly record struct ReplicaBootstrapOfferResult(bool Accepted, GroupMemberState? MemberState, string RefusalCode);
