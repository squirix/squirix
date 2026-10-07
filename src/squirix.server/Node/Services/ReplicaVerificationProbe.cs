using System;
using System.Collections.Generic;
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
/// Followers are probed without the gate, so a dead or slow peer never delays writes. The verdicts are admitted afterwards under the
/// commit gate of the committer, and the followers that answered but lack entries are handed to the catch-up pass.
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

    /// <summary>The followers the last admitted verification found answering, and the pipeline lookup of their catch-up target.</summary>
    private CatchUpOffer? _catchUp;

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
        Repairs = new ReplicaRepairQueue(locator.ReplicaCount);
    }

    /// <summary>Gets the follower slots the commit path demoted, waiting for the readiness service to verify and catch them up.</summary>
    internal ReplicaRepairQueue Repairs { get; }

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
                ServerLog.ReplicaTailOfOlderTerm(_log, tail.LastIndex, term);

            return new ReplicaVerificationSnapshot(ReplicaVerification.Blocked);
        }

        _ = ReportBlockedTail(null);

        if (tail.IsEmpty && eligibility.AllCanCountInWriteQuorum())
            return new ReplicaVerificationSnapshot(ReplicaVerification.AllReady);

        var (members, header) = BuildMembership(term);
        var probed = await ReplicaReadinessProbe.ProbeAllAsync(_gateway, ReplicaReadinessProbe.NonReadyFollowers(eligibility), members, header, status, ProbeTimeout, cancellationToken)
                                               .ConfigureAwait(false);
        var answered = new bool[probed.Length];
        var anyAnswered = false;
        for (var i = 1; i < probed.Length; i++)
        {
            answered[i] = probed[i].Kind == ReplicaProbeKind.Accepted || probed[i].Kind == ReplicaProbeKind.LogMismatch;
            anyAnswered |= answered[i];
        }

        return !anyAnswered && !eligibility.AllCanCountInWriteQuorum()
            ? new ReplicaVerificationSnapshot(ReplicaVerification.Pending)
            : new ReplicaVerificationSnapshot(in status, probed, answered, members, in header);
    }

    /// <summary>Re-probes the verified slots when the leader tail moved, admits the verdicts into the eligibility, and applies them to the coordinator.</summary>
    /// <param name="log">The owned group log.</param>
    /// <param name="snapshot">The follower probing taken outside the commit gate.</param>
    /// <param name="coordinator">The running coordinator the verified slots are admitted into.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The eligibility of the owned group after the verdicts were applied.</returns>
    /// <remarks>Runs under the commit gate of the committer.</remarks>
    internal async Task<ReplicaEligibility> AdmitVerifiedSlotsAsync(
        IFollowerLog log,
        ReplicaVerificationSnapshot snapshot,
        ReplicaCommitCoordinator coordinator,
        CancellationToken cancellationToken)
    {
        var status = snapshot.Status;
        var probed = snapshot.Probed;
        var current = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);

        // A commit may have moved the tail between the unguarded probe and the gate: the verdicts then describe
        // an older tail, so the slots that answered are probed again against the current one.
        if (current.LastLogIndex != status.LastLogIndex || current.LastLogTerm != status.LastLogTerm)
            probed = await ReplicaReadinessProbe.ProbeAllAsync(_gateway, snapshot.Answered, snapshot.Members, snapshot.Header, current, ProbeTimeout, cancellationToken).ConfigureAwait(false);

        // StartAsync may have verified some of these slots while this call waited for the gate: an older verdict
        // must not demote them.
        var eligibility = _registry.EligibilityFor(_groupId);
        for (var i = 1; i < probed.Length; i++)
        {
            if (eligibility.CanCountInWriteQuorum(i))
                probed[i] = default;
        }

        ReplicaReadinessProbe.ApplyAll(eligibility, probed, in current, _topologyFingerprint, _generation, coordinator);
        return eligibility;
    }

    /// <summary>Records the followers an admitted verification found answering, for the next catch-up pass.</summary>
    /// <param name="answered">Per-slot flags of the followers that answered their probe.</param>
    /// <param name="targetFor">Resolves the catch-up target of a slot in the pipeline the verification ran against.</param>
    /// <remarks>Runs under the commit gate of the committer, after the verdicts were applied.</remarks>
    internal void OfferCatchUp(bool[] answered, Func<int, ReplicaCatchUpTarget> targetFor) => Volatile.Write(ref _catchUp, new CatchUpOffer(answered, targetFor));

    /// <summary>Takes the followers to catch up: those the last admitted verification found answering that are still catching up.</summary>
    /// <returns>The catch-up targets in slot order; empty when no verification was admitted since the last call.</returns>
    /// <remarks>Each admitted verification feeds one catch-up pass, so a follower is caught up again only after it answered a new probe.</remarks>
    internal List<ReplicaCatchUpTarget> TakeCatchUpTargets()
    {
        var targets = new List<ReplicaCatchUpTarget>();
        if (Interlocked.Exchange(ref _catchUp, null) is not { } offer)
            return targets;

        var eligibility = _registry.EligibilityFor(_groupId);
        for (var i = 1; i < offer.Answered.Length; i++)
        {
            if (offer.Answered[i] && eligibility.StateFor(i) == ReplicaParticipantState.CatchingUp)
                targets.Add(offer.TargetFor(i));
        }

        return targets;
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

    /// <summary>The followers an admitted verification found answering, with the target lookup of the pipeline it ran against.</summary>
    /// <param name="Answered">Per-slot flags of the followers that answered their probe.</param>
    /// <param name="TargetFor">Resolves the catch-up target of a slot.</param>
    [Immutable]
    private sealed record CatchUpOffer(bool[] Answered, Func<int, ReplicaCatchUpTarget> TargetFor);
}
