using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>The first start of the replica group this node owns: seeds the copy of the owner once every other member can be seeded with it.</summary>
/// <remarks>
/// <para>
/// A copy of a group on a fresh disk is unseeded and holds no data of the group. Only the owner of a group may complete its own copy without
/// seeing a complete member, and only when every other member answered with the same topology, a ready log, and an unseeded copy: then no
/// member holds data the empty copy could contradict. The owner offers every member a place in the start, and completes its own copy once
/// all of them accepted. The offered members complete on the first call of a complete member, which the leadership of the owner sends.
/// </para>
/// <para>
/// <see cref="RunOnceAsync" /> runs one iteration and never waits: the caller decides when to run the next. A member that did not answer in
/// time, or an iteration that ended before every offer was accepted, leaves nothing but harmless offers, which a later iteration repeats.
/// </para>
/// </remarks>
internal sealed class ReplicaGroupBootstrap
{
    /// <summary>The refusal code reported for an offer that got no answer.</summary>
    private const string NoAnswer = "no-answer";

    private readonly TimeSpan _callTimeout;
    private readonly IReplicaBootstrapGateway _gateway;
    private readonly ReplicaRpcHeader _header;
    private readonly IFollowerLog _log;
    private readonly string[] _others;

    /// <summary>Initializes a new instance of the <see cref="ReplicaGroupBootstrap" /> class.</summary>
    /// <param name="log">The log of the group on this node.</param>
    /// <param name="gateway">The RPCs to the other members.</param>
    /// <param name="members">The members of the group, this node among them.</param>
    /// <param name="header">The envelope of the RPCs: group, topology, and this node as sender.</param>
    /// <param name="callTimeout">The longest wait for one member to answer one call.</param>
    /// <exception cref="ArgumentException">The group is not owned by the sender, or the sender is not a member of it.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="callTimeout" /> is not positive.</exception>
    internal ReplicaGroupBootstrap(IFollowerLog log, IReplicaBootstrapGateway gateway, string[] members, in ReplicaRpcHeader header, TimeSpan callTimeout)
    {
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(gateway);
        ArgumentNullException.ThrowIfNull(members);
        ArgumentException.ThrowIfNullOrWhiteSpace(header.GroupId);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(callTimeout, TimeSpan.Zero);
        if (!string.Equals(header.GroupId, header.SenderNodeId, StringComparison.Ordinal))
            throw new ArgumentException("Only the owner of a group starts it.", nameof(header));

        var others = new List<string>(members.Length);
        foreach (var member in members)
        {
            if (!string.Equals(member, header.SenderNodeId, StringComparison.Ordinal))
                others.Add(member);
        }

        if (others.Count == members.Length)
            throw new ArgumentException($"Node '{header.SenderNodeId}' is not a member of replica group '{header.GroupId}'.", nameof(members));

        _log = log;
        _gateway = gateway;
        _others = [.. others];
        _header = header with { Term = 0, LeaderNodeId = string.Empty };
        _callTimeout = callTimeout;
    }

    /// <summary>Gets the replica group identifier, which is the identifier of this node.</summary>
    internal string GroupId => _header.GroupId;

    /// <summary>Runs one iteration of the first start.</summary>
    /// <param name="cancellationToken">Cancellation token; its cancellation ends the iteration by throwing.</param>
    /// <returns>What the iteration did, with the member it concerns.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken" /> was canceled.</exception>
    internal async Task<ReplicaBootstrapOutcome> RunOnceAsync(CancellationToken cancellationToken)
    {
        var status = await _log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        if (Settled(in status) is { } settled)
            return settled;

        var statuses = new Task<ReplicaBootstrapOutcome?>[_others.Length];
        for (var i = 0; i < statuses.Length; i++)
            statuses[i] = InspectAsync(_others[i], cancellationToken);

        if (await FirstProblemAsync(statuses).ConfigureAwait(false) is { } waiting)
            return waiting;

        var offers = new Task<ReplicaBootstrapOutcome?>[_others.Length];
        for (var i = 0; i < offers.Length; i++)
            offers[i] = OfferAsync(_others[i], cancellationToken);

        if (await FirstProblemAsync(offers).ConfigureAwait(false) is { } refused)
            return refused;

        await _log.CompleteBootstrapAsync(cancellationToken).ConfigureAwait(false);
        return new ReplicaBootstrapOutcome(ReplicaBootstrapResult.Completed, string.Empty);
    }

    /// <summary>Tells whether the state of the own copy settles the iteration before anybody is asked.</summary>
    /// <param name="status">The own log status.</param>
    /// <returns>The outcome, or <see langword="null" /> when the copy is unseeded and ready, so the members are asked.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The state is not a named value.</exception>
    private static ReplicaBootstrapOutcome? Settled(in FollowerLogStatus status) => status.MemberState switch
    {
        GroupMemberState.Complete => new ReplicaBootstrapOutcome(ReplicaBootstrapResult.AlreadyComplete, string.Empty),
        GroupMemberState.Incomplete => new ReplicaBootstrapOutcome(ReplicaBootstrapResult.Incomplete, string.Empty),
        GroupMemberState.Unseeded => status.Readiness == FollowerLogReadiness.Ready ? null : new ReplicaBootstrapOutcome(ReplicaBootstrapResult.LogNotReady, string.Empty),
        _ => throw new ArgumentOutOfRangeException(nameof(status), status.MemberState, "Unsupported group member state."),
    };

    /// <summary>Waits for the calls to every member and picks the problem to report.</summary>
    /// <param name="calls">The calls, in member order; each ends with the problem it found, or <see langword="null" />.</param>
    /// <returns>A member that holds a complete copy if there is one, otherwise the first problem in member order; <see langword="null" /> when there is none.</returns>
    private static async Task<ReplicaBootstrapOutcome?> FirstProblemAsync(Task<ReplicaBootstrapOutcome?>[] calls)
    {
        var problems = await Task.WhenAll(calls).ConfigureAwait(false);
        ReplicaBootstrapOutcome? first = null;
        foreach (var problem in problems)
        {
            if (problem?.Result == ReplicaBootstrapResult.MemberHoldsCompleteCopy)
                return problem;

            first ??= problem;
        }

        return first;
    }

    /// <summary>Reads the status of one member and judges whether the member can be seeded.</summary>
    /// <param name="nodeId">The member.</param>
    /// <param name="cancellationToken">Cancellation token; its cancellation propagates.</param>
    /// <returns>The problem found, or <see langword="null" /> when the member is ready, unseeded, and of the same topology.</returns>
    private async Task<ReplicaBootstrapOutcome?> InspectAsync(string nodeId, CancellationToken cancellationToken)
    {
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        bounded.CancelAfter(_callTimeout);
        try
        {
            var status = await _gateway.GetStatusAsync(nodeId, _header, bounded.Token).ConfigureAwait(false);
            return Judge(nodeId, in status);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return new ReplicaBootstrapOutcome(ReplicaBootstrapResult.WaitingForMember, nodeId);
        }
        catch (Exception exception) when (exception is RpcException or IOException or HttpRequestException or TimeoutException or InvalidOperationException)
        {
            // A call the stopping token canceled can surface as an RpcException: it ends the iteration like any other cancellation.
            cancellationToken.ThrowIfCancellationRequested();

            // A member that is down or slow is not a fault of the owner: the start waits for it.
            return new ReplicaBootstrapOutcome(ReplicaBootstrapResult.WaitingForMember, nodeId);
        }
    }

    private ReplicaBootstrapOutcome? Judge(string nodeId, in ReplicaMemberStatus status)
    {
        if (status.MemberState == GroupMemberState.Complete)
            return new ReplicaBootstrapOutcome(ReplicaBootstrapResult.MemberHoldsCompleteCopy, nodeId);

        var answered = status.Ready && string.IsNullOrEmpty(status.RefusalCode) && status.MemberState == GroupMemberState.Unseeded;
        var sameTopology = status.ConfigurationGeneration == _header.ConfigurationGeneration && status.TopologyFingerprint.Span.SequenceEqual(_header.TopologyFingerprint.Span);
        return answered && sameTopology ? null : new ReplicaBootstrapOutcome(ReplicaBootstrapResult.WaitingForMember, nodeId);
    }

    /// <summary>Offers one member a place in the start.</summary>
    /// <param name="nodeId">The member.</param>
    /// <param name="cancellationToken">Cancellation token; its cancellation propagates.</param>
    /// <returns>The refusal, or <see langword="null" /> when the member accepted.</returns>
    private async Task<ReplicaBootstrapOutcome?> OfferAsync(string nodeId, CancellationToken cancellationToken)
    {
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        bounded.CancelAfter(_callTimeout);
        try
        {
            var result = await _gateway.OfferAsync(nodeId, _header, bounded.Token).ConfigureAwait(false);
            return result.Accepted ? null : new ReplicaBootstrapOutcome(ReplicaBootstrapResult.OfferRefused, nodeId, result.MemberState, result.RefusalCode);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return new ReplicaBootstrapOutcome(ReplicaBootstrapResult.OfferRefused, nodeId, null, NoAnswer);
        }
        catch (Exception exception) when (exception is RpcException or IOException or HttpRequestException or TimeoutException or InvalidOperationException)
        {
            // A call the stopping token canceled can surface as an RpcException: it ends the iteration like any other cancellation.
            cancellationToken.ThrowIfCancellationRequested();

            // An offer that did not arrive is not accepted; one that did is harmless and repeated by the next iteration.
            return new ReplicaBootstrapOutcome(ReplicaBootstrapResult.OfferRefused, nodeId, null, NoAnswer);
        }
    }
}
