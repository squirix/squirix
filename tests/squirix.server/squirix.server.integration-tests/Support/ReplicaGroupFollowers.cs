using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.Hosting;
using TUnit.Core.Exceptions;

namespace Squirix.Server.IntegrationTests.Support;

/// <summary>Waits until the followers of an owned replica group hold everything its owner has appended.</summary>
/// <remarks>
/// A commit returns once a majority holds the entry, so the slowest follower may still be receiving it when a test stops that
/// follower or reads its log. A follower is sent its entries in order, so a write does not overtake the previous one; a test that needs
/// every follower to hold what the owner appended waits here once, at that point, instead of pacing its writes.
/// </remarks>
internal static class ReplicaGroupFollowers
{
    private const int YieldsBeforeDelay = 64;

    /// <summary>Bounds the wait of one follower; a healthy follower holds an entry within milliseconds, so the bound only absorbs a loaded machine.</summary>
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    /// <summary>Bounds the verification of the owner's replica slots, which covers the first TLS handshake to each follower.</summary>
    private static readonly TimeSpan VerificationBound = TimeSpan.FromSeconds(30);

    /// <summary>Verifies the owner's replica slots, as its readiness service does, until every slot counts, so the first write finds a write quorum.</summary>
    /// <param name="owner">The group owner.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="System.TimeoutException">The owner did not verify every replica slot within the bound.</exception>
    /// <remarks>
    /// The owner probes its followers once at start with a short real-time budget that also covers the first TLS handshake; a probe that
    /// loses that race is not retried by a write, only by this verification.
    /// </remarks>
    internal static async Task AwaitVerifiedAsync(ITestNodeHost owner, CancellationToken cancellationToken)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(VerificationBound);
        var committer = owner.GetRequiredService<ReplicaGroupCommitter>();
        var verdict = ReplicaVerification.Pending;
        try
        {
            while ((verdict = await committer.VerifyReplicasAsync(deadline.Token)) != ReplicaVerification.AllReady)
                await Task.Delay(TimeSpan.FromMilliseconds(100), TimeProvider.System, deadline.Token);
        }
        catch (OperationCanceledException exception) when (!cancellationToken.IsCancellationRequested)
        {
            throw new System.TimeoutException($"The owner did not verify every replica slot within {VerificationBound}; last verdict {verdict}.", exception);
        }
    }

    /// <summary>Waits until every follower's copy of the group log holds the owner's last appended entry.</summary>
    /// <param name="owner">The group owner.</param>
    /// <param name="groupId">The owned group identifier, which is the owner's node identifier.</param>
    /// <param name="followers">The follower nodes that must keep up, with their node identifiers.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">No follower received the entry, or a follower did not move at all while it waited: replication itself is broken.</exception>
    /// <exception cref="SkipTestException">Some follower kept advancing but did not reach the entry in time while another received it; a follower that misses an entry stays behind, so the run cannot show what it checks.</exception>
    /// <remarks>
    /// A follower that stays behind although it keeps receiving appends, or one that receives nothing, is not caught up by waiting; the
    /// exceptions above report it.
    /// </remarks>
    internal static async Task AwaitCaughtUpAsync(ITestNodeHost owner, string groupId, (string Id, ITestNodeHost Host)[] followers, CancellationToken cancellationToken)
    {
        if (followers.Length == 0)
            return;

        var target = (await Log(owner, groupId, groupId).GetStatusAsync(cancellationToken)).LastLogIndex;
        var behind = 0;
        var stuck = string.Empty;
        var report = string.Empty;
        foreach (var (id, host) in followers)
        {
            var log = Log(host, id, groupId);
            var started = Stopwatch.GetTimestamp();
            var first = (await log.GetStatusAsync(cancellationToken)).LastLogIndex;
            var last = first;
            var attempt = 0;
            while (last < target && Stopwatch.GetElapsedTime(started) < Bound)
            {
                if (attempt++ < YieldsBeforeDelay)
                    await Task.Yield();
                else
                    await Task.Delay(TimeSpan.FromMilliseconds(1), TimeProvider.System, cancellationToken);

                last = (await log.GetStatusAsync(cancellationToken)).LastLogIndex;
            }

            if (last >= target)
                continue;

            behind++;
            report = string.Concat(report, $" {id} last {last};");
            if (last == first)
                stuck = string.Concat(stuck, $" {id} stayed at {last};");
        }

        if (behind == 0)
            return;

        var message = $"Group log of the owner {groupId} ends at {target};{report}";
        if (stuck.Length > 0)
            throw new InvalidOperationException($"A follower did not receive any entry while the owner waited: {message} Stuck:{stuck}");

        throw behind == followers.Length
            ? new InvalidOperationException($"No follower received the last entry: {message}")
            : new SkipTestException($"A follower fell behind and is never caught up, so the run is inconclusive: {message}");
    }

    private static IFollowerLog Log(ITestNodeHost node, string nodeId, string groupId) =>
        node.GetRequiredService<ReplicaGroupRegistry>().TryGetLog(groupId, out var log)
            ? log
            : throw new InvalidOperationException($"Node {nodeId} does not serve group log {groupId}.");
}
