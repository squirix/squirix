using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>An RF=3 group owner hands each follower its entries in log order, whatever the other follower does.</summary>
public sealed class ReplicaFollowerOrderingTests : IsolatedStorageTestBase
{
    private const string GroupId = "n1";

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>
    /// A follower whose append of entry N is slow is not sent entry N+1 before it answers, so it ends holding both without refusing
    /// either, while the group commits both at the majority of the other follower.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SlowFollowerGetsAppendsInOrder(CancellationToken cancellationToken)
    {
        await using var followers = await OpenFollowersAsync(cancellationToken);
        var gateway = new FollowerLogRoutingGateway(followers.Logs);
        await using var registry = await OpenRegistryAsync(Path.Join(Dir, "owner"), cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);
        try
        {
            await committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry("k1"), cancellationToken);
            await gateway.AppendedAsync("n3", 1).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            gateway.ParkNext("n3");
            await committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry("k2"), cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            await gateway.Parked.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            await committer.CommitSetAsync(NewOperationId(), "cache", "k3", Entry("k3"), cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            var overtaken = 0;
            foreach (var arrival in gateway.Arrivals)
            {
                if (string.Equals(arrival.Node, "n3", StringComparison.Ordinal) && arrival.LastIndex >= 3)
                    overtaken++;
            }

            _ = await Assert.That(overtaken).IsEqualTo(0);

            gateway.Release();
            await gateway.AppendedAsync("n3", 3).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            _ = await Assert.That(gateway.Refusals.IsEmpty).IsTrue();
            _ = await Assert.That((await followers.Logs["n3"].GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(3UL);
        }
        finally
        {
            gateway.Release();
        }
    }

    /// <summary>A follower that does not answer never holds up a commit: the other follower makes the majority.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SlowFollowerDoesNotDelayMajority(CancellationToken cancellationToken)
    {
        await using var followers = await OpenFollowersAsync(cancellationToken);
        var gateway = new FollowerLogRoutingGateway(followers.Logs);
        await using var registry = await OpenRegistryAsync(Path.Join(Dir, "owner"), cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);
        try
        {
            gateway.ParkNext("n3");
            for (var i = 1; i <= 3; i++)
            {
                var key = $"k{i}";
                await committer.CommitSetAsync(NewOperationId(), "cache", key, Entry(key), cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            }

            _ = await Assert.That((await followers.Logs["n2"].GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(3UL);
            _ = await Assert.That((await followers.Logs["n3"].GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(0UL);
        }
        finally
        {
            gateway.Release();
        }
    }

    /// <summary>Disposing the owner cancels the append a follower never answered and returns, instead of leaving the call parked.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeClosesFollowerSenders(CancellationToken cancellationToken)
    {
        await using var followers = await OpenFollowersAsync(cancellationToken);
        var gateway = new FollowerLogRoutingGateway(followers.Logs);
        await using var registry = await OpenRegistryAsync(Path.Join(Dir, "owner"), cancellationToken);
        var committer = CreateCommitter(registry, gateway);
        try
        {
            gateway.ParkNext("n3");
            await committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry("k1"), cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            await gateway.Parked.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            await committer.DisposeAsync().AsTask().WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            await gateway.ParkCanceled.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            gateway.Release();
            await committer.DisposeAsync();
        }
    }

    /// <summary>
    /// An in-process restart of the coordinator, after a write failed at its local append, delivers the appends still queued for a slow
    /// follower before the new pipeline sends it anything, so the follower stays in step without a refusal.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QueuedAppendsSurviveRestart(CancellationToken cancellationToken)
    {
        await using var followers = await OpenFollowersAsync(cancellationToken);
        var gateway = new FollowerLogRoutingGateway(followers.Logs);
        using var hooks = new StallableFollowerLogFaultHooks();
        await using var registry = await OpenRegistryAsync(Path.Join(Dir, "owner"), new FollowerLogOptions { FaultHooks = hooks }, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);
        try
        {
            gateway.ParkNext("n3");
            await committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry("k1"), cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            await gateway.Parked.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            await committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry("k2"), cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            // The next write fails at its local append, which drops the started state: the write after it restarts the coordinator.
            hooks.StallNextFrameWrite();
            var failing = committer.CommitSetAsync(NewOperationId(), "cache", "k3", Entry("k3"), cancellationToken);
            await hooks.Entered.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            hooks.ReleaseWithFailure(new IOException("Injected frame write failure."));
            _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(failing.WaitAsync(HangGuard, TimeProvider.System, cancellationToken));

            var restarting = committer.CommitSetAsync(NewOperationId(), "cache", "k4", Entry("k4"), cancellationToken);
            gateway.Release();
            await restarting.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            var last = (await StatusAsync(registry, cancellationToken)).LastLogIndex;
            await gateway.AppendedAsync("n3", last).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            _ = await Assert.That(gateway.Refusals.IsEmpty).IsTrue();
            _ = await Assert.That((await followers.Logs["n3"].GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(last);
        }
        finally
        {
            gateway.Release();
            hooks.Release();
        }
    }

    /// <summary>A failure while closing a follower sender does not stop the owner from disposing the rest of its pipeline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeSurvivesSenderCloseFailure(CancellationToken cancellationToken)
    {
        await using var followers = await OpenFollowersAsync(cancellationToken);
        var gateway = new FollowerLogRoutingGateway(followers.Logs) { FailsOnCancel = true };
        await using var registry = await OpenRegistryAsync(Path.Join(Dir, "owner"), cancellationToken);
        var committer = CreateCommitter(registry, gateway);
        try
        {
            gateway.ParkNext("n3");
            await committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry("k1"), cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            await gateway.Parked.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            await committer.DisposeAsync().AsTask().WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            await gateway.ParkCanceled.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            gateway.Release();
            await committer.DisposeAsync();
        }
    }

    private async Task<Followers> OpenFollowersAsync(CancellationToken cancellationToken)
    {
        var followers = new Followers();
        try
        {
            foreach (var node in new[] { "n2", "n3" })
            {
                var log = new FollowerLog(Path.Join(Dir, node), GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance);
                followers.Logs[node] = log;
                await log.OpenAsync(cancellationToken);
            }
        }
        catch
        {
            await followers.DisposeAsync();
            throw;
        }

        return followers;
    }

    /// <summary>The follower logs of the group, disposed together.</summary>
    private sealed class Followers : IAsyncDisposable
    {
        internal ConcurrentDictionary<string, FollowerLog> Logs { get; } = new(StringComparer.Ordinal);

        public async ValueTask DisposeAsync()
        {
            foreach (var log in Logs.Values)
                await log.DisposeAsync();
        }
    }
}
