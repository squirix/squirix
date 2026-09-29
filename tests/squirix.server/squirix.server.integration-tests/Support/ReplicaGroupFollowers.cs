using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.Hosting;
using TUnit.Core.Exceptions;

namespace Squirix.Server.IntegrationTests.Support;

/// <summary>Waits until the followers of an owned replica group hold everything its owner has appended.</summary>
/// <remarks>
/// A commit returns once a majority holds the entry, so the slowest follower may still be receiving it when the next write
/// starts, or when a test stops the follower. A follower that misses an entry is never caught up again, so a test that needs every
/// follower to keep up waits here after each write instead of relying on the timing of the runner.
/// </remarks>
internal static class ReplicaGroupFollowers
{
    private const int YieldsBeforeDelay = 64;

    /// <summary>Bounds the wait of one follower; a healthy follower holds an entry within milliseconds.</summary>
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    /// <summary>Waits until every follower's copy of the group log holds the owner's last appended entry.</summary>
    /// <param name="owner">The group owner.</param>
    /// <param name="groupId">The owned group identifier, which is the owner's node identifier.</param>
    /// <param name="followers">The follower nodes that must keep up.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">No follower received the entry: replication itself is broken.</exception>
    /// <exception cref="SkipTestException">Some follower fell behind while another received the entry; the follower stays behind, so the run cannot show what it checks.</exception>
    internal static async Task AwaitCaughtUpAsync(ITestNodeHost owner, string groupId, ITestNodeHost[] followers, CancellationToken cancellationToken)
    {
        if (followers.Length == 0)
            return;

        var target = (await Log(owner, groupId).GetStatusAsync(cancellationToken)).LastLogIndex;
        var behind = 0;
        var report = string.Empty;
        foreach (var follower in followers)
        {
            var log = Log(follower, groupId);
            var started = Stopwatch.GetTimestamp();
            var last = (await log.GetStatusAsync(cancellationToken)).LastLogIndex;
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
            report = string.Concat(report, $" follower last {last};");
        }

        if (behind == 0)
            return;

        var message = $"Group log of the owner ends at {target};{report}";
        throw behind == followers.Length
            ? new InvalidOperationException($"No follower received the last entry: {message}")
            : new SkipTestException($"A follower fell behind and is never caught up, so the run is inconclusive: {message}");
    }

    private static IFollowerLog Log(ITestNodeHost node, string groupId)
    {
        _ = node.GetRequiredService<ReplicaGroupRegistry>().TryGetLog(groupId, out var log);
        return log!;
    }
}
