using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>The outcome of one follower append observation: the follower's slot and its acknowledgement, or none when its task faulted or was canceled.</summary>
/// <param name="ReplicaIndex">Zero-based replica slot of the follower.</param>
/// <param name="Acknowledgement">The follower's durable acknowledgement, or <see langword="null" />.</param>
[Immutable]
internal readonly record struct FollowerCompletion(int ReplicaIndex, ReplicaDurableAcknowledgement? Acknowledgement);
