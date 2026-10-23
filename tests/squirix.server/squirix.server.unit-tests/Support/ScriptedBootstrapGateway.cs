using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Members of a group that answer the first-start calls as the test scripts them, recording the calls in order.</summary>
/// <remarks>A stateful fake: the test changes what a member reports between iterations, and the calls are recorded across them.</remarks>
[ThreadSafe]
internal sealed class ScriptedBootstrapGateway : IReplicaBootstrapGateway
{
    private readonly ConcurrentQueue<string> _calls = new();
    private readonly ConcurrentDictionary<string, Member> _members = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _statusCalls = new(0);

    /// <summary>Gets the calls in the order they arrived, as <c language="text">status:node</c> and <c language="text">offer:node</c>.</summary>
    internal string Calls => string.Join(',', _calls);

    /// <summary>Gets the number of offers sent so far.</summary>
    internal int Offers
    {
        get
        {
            var offers = 0;
            foreach (var call in _calls)
            {
                if (call.StartsWith("offer:", StringComparison.Ordinal))
                    offers++;
            }

            return offers;
        }
    }

    /// <inheritdoc />
    public Task<ReplicaMemberStatus> GetStatusAsync(string nodeId, ReplicaRpcHeader header, CancellationToken cancellationToken)
    {
        _calls.Enqueue("status:" + nodeId);
        _ = _statusCalls.Release();
        if (!_members.TryGetValue(nodeId, out var member) || member.Down)
            return Task.FromException<ReplicaMemberStatus>(new IOException("member unreachable"));

        member.StatusCallback?.Invoke();
        return member.StatusFault == null ? Task.FromResult(member.Status) : Task.FromException<ReplicaMemberStatus>(member.StatusFault);
    }

    /// <inheritdoc />
    public Task<ReplicaBootstrapOfferResult> OfferAsync(string nodeId, ReplicaRpcHeader header, CancellationToken cancellationToken)
    {
        _calls.Enqueue("offer:" + nodeId);
        if (!_members.TryGetValue(nodeId, out var member) || member.Down)
            return Task.FromException<ReplicaBootstrapOfferResult>(new IOException("member unreachable"));

        member.OfferCallback?.Invoke();
        var answer = member.Refuses
            ? new ReplicaBootstrapOfferResult(false, member.Status.MemberState, FollowerLogRefusal.NotReady)
            : new ReplicaBootstrapOfferResult(true, member.Status.MemberState, string.Empty);
        return member.OfferFault == null ? Task.FromResult(answer) : Task.FromException<ReplicaBootstrapOfferResult>(member.OfferFault);
    }

    /// <summary>Sets the status a member reports; the member is reachable and accepts offers.</summary>
    /// <param name="nodeId">The member.</param>
    /// <param name="status">The status.</param>
    internal void Report(string nodeId, in ReplicaMemberStatus status) => _members[nodeId] = new Member { Status = status };

    /// <summary>Makes a member unreachable.</summary>
    /// <param name="nodeId">The member.</param>
    internal void Down(string nodeId) => _members[nodeId] = new Member { Status = default, Down = true };

    /// <summary>Makes a member refuse every offer.</summary>
    /// <param name="nodeId">The member.</param>
    internal void RefuseOffers(string nodeId) => _members[nodeId].Refuses = true;

    /// <summary>Makes the status call of a member run a callback and then fail with an exception.</summary>
    /// <param name="nodeId">The member.</param>
    /// <param name="exception">The exception the call fails with.</param>
    /// <param name="callback">What runs when the call arrives, before it fails.</param>
    internal void FailStatus(string nodeId, Exception exception, Action callback) =>
        _members[nodeId] = new Member { Status = default, StatusFault = exception, StatusCallback = callback };

    /// <summary>Makes the offer call of a reachable member run a callback and then fail with an exception.</summary>
    /// <param name="nodeId">The member.</param>
    /// <param name="exception">The exception the call fails with.</param>
    /// <param name="callback">What runs when the call arrives, before it fails.</param>
    internal void FailOffer(string nodeId, Exception exception, Action callback)
    {
        var member = _members[nodeId];
        member.OfferFault = exception;
        member.OfferCallback = callback;
    }

    /// <summary>Waits until a number of status calls arrived since the last wait consumed them.</summary>
    /// <param name="count">The number of status calls to wait for.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes once the calls arrived.</returns>
    internal async Task WaitForStatusCallsAsync(int count, CancellationToken cancellationToken)
    {
        for (var i = 0; i < count; i++)
            await _statusCalls.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    [Mutable]
    private sealed class Member
    {
        internal required ReplicaMemberStatus Status { get; init; }

        internal bool Down { get; init; }

        internal bool Refuses { get; set; }

        internal Action? OfferCallback { get; set; }

        internal Exception? OfferFault { get; set; }

        internal Action? StatusCallback { get; init; }

        internal Exception? StatusFault { get; init; }
    }
}
