using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>How the expiry batch runner reacts to a delegate that throws, to a bad key, and to a key already in flight.</summary>
public sealed class ReplicaExpirationCoordinatorTests : ServerUnitTestBase
{
    private const string CacheName = "default";

    /// <summary>A bad key of a group of keys is refused before any of them is queued, so no key waits in the queue without a runner.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BadKeyQueuesNoneOfTheGroup(CancellationToken cancellationToken)
    {
        var seen = new ConcurrentQueue<string>();
        await using var expiration = new ReplicaExpirationCoordinator<string>(keys =>
        {
            foreach (var (_, key) in keys)
                seen.Enqueue(key);

            return Task.FromResult(new (string? Entry, Exception? Failure)[keys.Count]);
        })
        { ShutdownTimeProvider = TimeProvider.System };

        var error = NodeExceptionAssert.For<ArgumentException>().Throws(
            expiration,
            static refused => _ = refused.ExpireMany([(CacheName, "key-a"), (CacheName, string.Empty)], CancellationToken.None));
        _ = await expiration.ExpireAsync(CacheName, "key-b", cancellationToken);

        _ = await Assert.That(error).IsNotNull();
        await SequenceAssert.EqualAsync(["key-b"], seen.ToArray(), StringComparer.Ordinal);
    }

    /// <summary>A caller of a key whose batch is in flight joins that batch: the delegate sees the key once and both callers get its result.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CallerOfInFlightKeyJoinsItsBatch(CancellationToken cancellationToken)
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sizes = new ConcurrentQueue<int>();
        await using var expiration = new ReplicaExpirationCoordinator<string>(async keys =>
        {
            sizes.Enqueue(keys.Count);
            _ = entered.TrySetResult();
            await release.Task.ConfigureAwait(false);
            return [("live", null)];
        })
        { ShutdownTimeProvider = TimeProvider.System };

        var first = expiration.ExpireAsync(CacheName, "key-a", cancellationToken);
        await entered.Task.WaitAsync(cancellationToken);
        var second = expiration.ExpireAsync(CacheName, "key-a", cancellationToken);
        release.SetResult();

        _ = await Assert.That((await first, await second)).IsEqualTo(("live", "live"));
        await SequenceAssert.EqualAsync([1], sizes.ToArray());
    }

    /// <summary>
    /// A delegate that throws before it returns a task fails the keys of its batch and the keys queued behind it, and the next expiry starts a
    /// new runner instead of waiting for one that ended.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SyncThrowRefusesBatchAndRecovers(CancellationToken cancellationToken)
    {
        var calls = 0;
        await using var expiration = new ReplicaExpirationCoordinator<string>(keys => Interlocked.Increment(ref calls) == 1
                ? throw new InvalidOperationException("refused synchronously")
                : Task.FromResult(new (string? Entry, Exception? Failure)[keys.Count]))
        { ShutdownTimeProvider = TimeProvider.System, MaxBatchKeys = 1 };

        var waits = expiration.ExpireMany([(CacheName, "key-a"), (CacheName, "key-b")], cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(waits[0].WaitAsync(cancellationToken));
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(waits[1].WaitAsync(cancellationToken));
        var later = await expiration.ExpireAsync(CacheName, "key-c", cancellationToken);

        _ = await Assert.That(later).IsNull();
        _ = await Assert.That(Volatile.Read(ref calls)).IsEqualTo(2);
    }
}
