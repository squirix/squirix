using System.Runtime.InteropServices;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>What a catch-up of one follower slot runs against.</summary>
/// <param name="ReplicaIndex">Zero-based follower slot.</param>
/// <param name="Pipeline">The running commit pipeline that owns the sender; admission is refused once another pipeline replaced it.</param>
/// <param name="Sender">The follower's sender, which the catch-up leases.</param>
/// <param name="Log">The leader log the entries are read from.</param>
/// <param name="Term">The leader term of the pipeline.</param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct ReplicaCatchUpTarget(int ReplicaIndex, IReplicaCommitPipeline Pipeline, ReplicaFollowerSender Sender, IFollowerLog Log, ulong Term);
