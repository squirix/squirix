using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>The outcome of one follower append observation: the follower's slot and its acknowledgement, or none when its task faulted or was canceled.</summary>
/// <param name="ReplicaIndex">Zero-based replica slot of the follower.</param>
/// <param name="Acknowledgement">The follower's durable acknowledgement of a single entry, or <see langword="null" />.</param>
/// <param name="Recorded">
/// Whether the acknowledgements of the follower for a run of entries were already recorded in log order, one by one as each completed,
/// so the completion carries nothing left to record.
/// </param>
[Immutable]
internal readonly record struct FollowerCompletion(int ReplicaIndex, ReplicaDurableAcknowledgement? Acknowledgement, bool Recorded = false);
