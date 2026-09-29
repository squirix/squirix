using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Probes the followers of the owned replica group for verification, without the commit gate.</summary>
/// <remarks>
/// Followers are probed, and re-sent the leader's uncommitted tail when they lack it, so a dead or slow peer never delays writes.
/// The verdicts are admitted afterwards under the commit gate of the committer.
/// </remarks>
internal sealed class ReplicaVerificationProbe
{
    /// <summary>Gets the longest wait for one follower to answer a verification probe.</summary>
    internal static readonly TimeSpan ProbeTimeout = TimeSpan.FromSeconds(1);

    private readonly ulong _generation;
    private readonly IReplicaRpcGateway _gateway;
    private readonly string _groupId;
    private readonly IReplicaGroupLocator _locator;
    private readonly ILogger _log;
    private readonly ReplicaGroupRegistry _registry;
    private readonly Lock _reportSync = new();
    private readonly ReadOnlyMemory<byte> _topologyFingerprint;

    /// <summary>The blocked older-term tail last reported, so the warning is logged once per blocked state, not on every verification pass.</summary>
    private BlockedTail? _reportedBlockedTail;

    /// <summary>Initializes a new instance of the <see cref="ReplicaVerificationProbe" /> class.</summary>
    /// <param name="registry">Replica group registry of this node.</param>
    /// <param name="locator">Replica group locator resolving the owned group members.</param>
    /// <param name="gateway">Follower replication RPCs.</param>
    /// <param name="groupId">The owned replica group identifier, which is this node's identifier.</param>
    /// <param name="topologyFingerprint">Static topology fingerprint.</param>
    /// <param name="generation">Static configuration generation.</param>
    /// <param name="log">Logger for the blocked tail report.</param>
    internal ReplicaVerificationProbe(
        ReplicaGroupRegistry registry,
        IReplicaGroupLocator locator,
        IReplicaRpcGateway gateway,
        string groupId,
        ReadOnlyMemory<byte> topologyFingerprint,
        ulong generation,
        ILogger log)
    {
        _registry = registry;
        _locator = locator;
        _gateway = gateway;
        _groupId = groupId;
        _topologyFingerprint = topologyFingerprint;
        _generation = generation;
        _log = log;
    }

    /// <summary>Builds the group members and the replication envelope identity for a term.</summary>
    /// <param name="term">The leader's current term.</param>
    /// <returns>The ordered members, index zero being this node, and the envelope header.</returns>
    internal (string[] Members, ReplicaRpcHeader Header) BuildMembership(ulong term)
    {
        var members = new string[_locator.ReplicaCount];
        _locator.GetReplicaGroup(_groupId, members);
        return (members, new ReplicaRpcHeader(_groupId, _topologyFingerprint, _generation, term, _groupId, _groupId));
    }

    /// <summary>Probes the non-ready followers against the leader log.</summary>
    /// <param name="log">The owned group log.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The final verification state, or the probing the admission under the commit gate continues from.</returns>
    internal async Task<ReplicaVerificationSnapshot> ProbeAsync(IFollowerLog log, CancellationToken cancellationToken)
    {
        var eligibility = _registry.EligibilityFor(_groupId);
        var read = await log.GetLeaderTailAsync(cancellationToken).ConfigureAwait(false);
        var status = read.Status;
        if (status.Readiness != FollowerLogReadiness.Ready)
            return new ReplicaVerificationSnapshot(ReplicaVerification.Blocked);

        var term = Math.Max(1UL, status.CurrentTerm);
        var tail = ReplicaLeaderTail.From(read);
        if (!tail.IsCommittableIn(term))
        {
            // Counting replicas must not commit it, and no current-term entry exists yet to commit it transitively. The state is
            // reported when it starts or changes; the readiness report keeps showing it as blocked on every pass.
            if (ReportBlockedTail(new BlockedTail(tail.LastIndex, term)))
                LogManager.ReplicaTailOfOlderTerm(_log, tail.LastIndex, term);

            return new ReplicaVerificationSnapshot(ReplicaVerification.Blocked);
        }

        _ = ReportBlockedTail(null);

        if (tail.IsEmpty && eligibility.AllCanCountInWriteQuorum())
            return new ReplicaVerificationSnapshot(ReplicaVerification.AllReady);

        var (members, header) = BuildMembership(term);
        var probed = await ReplicaReadinessProbe.ProbeAllAsync(_gateway, ReplicaReadinessProbe.NonReadyFollowers(eligibility), members, header, status, ProbeTimeout, cancellationToken)
                                               .ConfigureAwait(false);
        probed = await ReplicaReadinessProbe.RedriveTailAsync(_gateway, probed, members, header, tail, ProbeTimeout, cancellationToken).ConfigureAwait(false);
        var answered = new bool[probed.Length];
        var anyAnswered = false;
        for (var i = 1; i < probed.Length; i++)
        {
            answered[i] = probed[i].Kind == ReplicaProbeKind.Accepted || probed[i].Kind == ReplicaProbeKind.LogMismatch;
            anyAnswered |= answered[i];
        }

        return !anyAnswered && !eligibility.AllCanCountInWriteQuorum()
            ? new ReplicaVerificationSnapshot(ReplicaVerification.Pending)
            : new ReplicaVerificationSnapshot(status, probed, answered, members, header);
    }

    private bool ReportBlockedTail(BlockedTail? blocked)
    {
        lock (_reportSync)
        {
            var changed = _reportedBlockedTail != blocked;
            _reportedBlockedTail = blocked;
            return changed;
        }
    }

    /// <summary>A leader tail that cannot be committed yet because it holds no entry of the current term.</summary>
    /// <param name="LastIndex">The last index of the tail.</param>
    /// <param name="Term">The leader's current term.</param>
    [Immutable]
    private sealed record BlockedTail(ulong LastIndex, ulong Term);
}
