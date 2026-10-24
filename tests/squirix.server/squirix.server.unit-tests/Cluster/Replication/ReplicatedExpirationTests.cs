using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Leader expiry single-flight, bounded shutdown and tombstone identity tests.</summary>
[Immutable]
public sealed class ReplicatedExpirationTests : ServerUnitTestBase
{
    private static readonly AsyncLocal<string?> CallerScope = new();

    /// <summary>A caller that stops waiting leaves the shared expiry running; a later caller of the key joins it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CallerCancellationLeavesSharedRun(CancellationToken cancellationToken)
    {
        var release = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var runs = 0;
        await using var expiration = new ReplicaExpirationCoordinator<string>(PerKey((_, _) =>
        {
            _ = Interlocked.Increment(ref runs);
            return release.Task;
        }))
        { ShutdownTimeProvider = TimeProvider.System };
        using var leaving = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var first = expiration.ExpireAsync("default", "key-a", leaving.Token);
        await leaving.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException, string?>(new ValueTask<string?>(first));
        var second = expiration.ExpireAsync("default", "key-a", cancellationToken);
        release.SetResult("live");

        _ = await Assert.That(await second).IsEqualTo("live");
        _ = await Assert.That(Volatile.Read(ref runs)).IsEqualTo(1);
    }

    /// <summary>The shared run does not carry the ambient state of the caller that started it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SharedRunDropsCallerContext(CancellationToken cancellationToken)
    {
        await using var expiration = new ReplicaExpirationCoordinator<string>(PerKey(static (_, _) => Task.FromResult(CallerScope.Value))) { ShutdownTimeProvider = TimeProvider.System };
        CallerScope.Value = "caller";

        var seen = await expiration.ExpireAsync("default", "key-a", cancellationToken);

        _ = await Assert.That(seen).IsNull();
    }

    /// <summary>Concurrent expiries of one key share a single result; another key is decided on its own.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentExpiriesShareOneRun(CancellationToken cancellationToken)
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var seen = new ConcurrentQueue<string>();
        await using var expiration = new ReplicaExpirationCoordinator<string>(async keys =>
        {
            foreach (var (_, key) in keys)
                seen.Enqueue(key);

            await release.Task;
            var results = new (string? Entry, Exception? Failure)[keys.Count];
            for (var i = 0; i < keys.Count; i++)
                results[i] = (string.Equals(keys[i].Key, "key-a", StringComparison.Ordinal) ? "live" : null, null);

            return results;
        })
        { ShutdownTimeProvider = TimeProvider.System };

        var first = expiration.ExpireAsync("default", "key-a", cancellationToken);
        var second = expiration.ExpireAsync("default", "key-a", cancellationToken);
        var other = expiration.ExpireAsync("default", "key-b", cancellationToken);
        release.SetResult();

        _ = await Assert.That(await first).IsEqualTo("live");
        _ = await Assert.That(await second).IsEqualTo("live");
        _ = await Assert.That(await other).IsNull();
        _ = await Assert.That(seen.Count).IsEqualTo(2);
    }

    /// <summary>Keys of distinct expiries that arrive while a batch runs are taken together by the next batch.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadsOfDistinctKeysShareBatch(CancellationToken cancellationToken)
    {
        const int Waiting = 5;
        using var batches = new BlockingBatches();
        await using var expiration = new ReplicaExpirationCoordinator<string>(batches.RunAsync) { ShutdownTimeProvider = TimeProvider.System };
        var head = expiration.ExpireAsync("default", "head", cancellationToken);
        await batches.Entered.WaitAsync(cancellationToken);
        var waiting = new Task<string?>[Waiting];
        for (var i = 0; i < Waiting; i++)
            waiting[i] = expiration.ExpireAsync("default", "key-" + i.ToString(CultureInfo.InvariantCulture), cancellationToken);

        batches.Release();
        _ = await head;
        _ = await Task.WhenAll(waiting);

        await SequenceAssert.EqualAsync([1, Waiting], batches.Sizes.ToArray());
    }

    /// <summary>Keys queued together are cut into batches of the batch limit, because the runner takes none before all are queued.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task KeysQueuedTogetherFormFullBatches(CancellationToken cancellationToken)
    {
        const int Limit = 4;
        const int Total = 10;
        using var batches = new BlockingBatches();
        batches.Release();
        await using var expiration = new ReplicaExpirationCoordinator<string>(batches.RunAsync) { ShutdownTimeProvider = TimeProvider.System, MaxBatchKeys = Limit };
        var keys = new (string CacheName, string Key)[Total];
        for (var i = 0; i < Total; i++)
            keys[i] = ("default", "key-" + i.ToString(CultureInfo.InvariantCulture));

        _ = await Task.WhenAll(expiration.ExpireMany(keys, cancellationToken));

        await SequenceAssert.EqualAsync([Limit, Limit, Total - (2 * Limit)], batches.Sizes.ToArray());
    }

    /// <summary>Keys queued but not yet taken fail at once with an object-disposed failure when the coordinator is disposed; the batch in flight finishes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QueuedExpiriesFailAtDispose(CancellationToken cancellationToken)
    {
        using var batches = new BlockingBatches();
        var expiration = new ReplicaExpirationCoordinator<string>(batches.RunAsync) { ShutdownTimeProvider = TimeProvider.System };
        var head = expiration.ExpireAsync("default", "head", cancellationToken);
        await batches.Entered.WaitAsync(cancellationToken);
        var queued = expiration.ExpireAsync("default", "queued", cancellationToken);

        var disposal = expiration.DisposeAsync().AsTask();
        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(queued.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken));
        _ = await Assert.That(disposal.IsCompleted).IsFalse();
        batches.Release();
        await disposal.WaitAsync(cancellationToken);

        _ = await Assert.That(await head).IsNull();
        _ = await Assert.That(batches.Sizes.Count).IsEqualTo(1);
    }

    /// <summary>A key that arrives at a full queue fails at once instead of waiting; the keys already queued still run.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FullQueueFailsOverflowKey(CancellationToken cancellationToken)
    {
        const int Capacity = 2;
        using var batches = new BlockingBatches();
        await using var expiration = new ReplicaExpirationCoordinator<string>(batches.RunAsync) { ShutdownTimeProvider = TimeProvider.System, MaxQueuedKeys = Capacity };
        var head = expiration.ExpireAsync("default", "head", cancellationToken);
        await batches.Entered.WaitAsync(cancellationToken);
        var first = expiration.ExpireAsync("default", "k1", cancellationToken);
        var second = expiration.ExpireAsync("default", "k2", cancellationToken);

        var overflow = expiration.ExpireAsync("default", "k3", cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(overflow);
        batches.Release();

        _ = await head;
        _ = await first;
        _ = await second;
        await SequenceAssert.EqualAsync([1, Capacity], batches.Sizes.ToArray());
    }

    /// <summary>A batch refused as a whole fails its keys and the keys queued behind it, and the delegate is not asked again.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusedBatchDoesNotSpin(CancellationToken cancellationToken)
    {
        using var batches = new BlockingBatches { Refusal = new InvalidOperationException("refused") };
        await using var expiration = new ReplicaExpirationCoordinator<string>(batches.RunAsync) { ShutdownTimeProvider = TimeProvider.System };
        var head = expiration.ExpireAsync("default", "head", cancellationToken);
        await batches.Entered.WaitAsync(cancellationToken);
        var queued = expiration.ExpireAsync("default", "queued", cancellationToken);

        batches.Release();

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(head);
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(queued);
        _ = await Assert.That(batches.Sizes.Count).IsEqualTo(1);
    }

    /// <summary>A live key of a batch hands its entry to its caller while the tombstone of another key of the same batch fails.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LiveKeyKeepsEntryBesideFailedKey(CancellationToken cancellationToken)
    {
        await using var expiration = new ReplicaExpirationCoordinator<string>(static keys =>
        {
            var results = new (string? Entry, Exception? Failure)[keys.Count];
            for (var i = 0; i < keys.Count; i++)
                results[i] = string.Equals(keys[i].Key, "live", StringComparison.Ordinal) ? ("entry", null) : (null, new InvalidOperationException("tombstone"));

            return Task.FromResult(results);
        })
        { ShutdownTimeProvider = TimeProvider.System };

        var live = expiration.ExpireAsync("default", "live", cancellationToken);
        var failed = expiration.ExpireAsync("default", "failed", cancellationToken);

        _ = await Assert.That(await live).IsEqualTo("entry");
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(failed);
    }

    /// <summary>Dispose of a coordinator whose expiry never ends completes once the budget elapses and reports the leak once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeBoundsHungExpiry(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        var budget = TimeSpan.FromMilliseconds(50);
        var leaks = 0;
        var reported = TimeSpan.Zero;
        var never = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var expiration = new ReplicaExpirationCoordinator<string>(PerKey((_, _) =>
        {
            _ = entered.TrySetResult();
            return never.Task;
        }))
        {
            ShutdownBudget = budget,
            ShutdownTimeProvider = clock,
            ShutdownLeakReporter = leaked =>
            {
                _ = Interlocked.Increment(ref leaks);
                reported = leaked;
            },
        };
        var hung = expiration.ExpireAsync("default", "key-a", cancellationToken);
        await entered.Task.WaitAsync(cancellationToken);

        var disposal = expiration.DisposeAsync().AsTask();
        _ = await Assert.That(disposal.IsCompleted).IsFalse();
        clock.Advance(budget);
        await disposal.WaitAsync(cancellationToken);
        await expiration.DisposeAsync();

        _ = await Assert.That(Volatile.Read(ref leaks)).IsEqualTo(1);
        _ = await Assert.That(reported).IsEqualTo(budget);
        _ = await Assert.That(hung.IsCompleted).IsFalse();
    }

    /// <summary>Dispose waits for an expiry in flight that ends within the budget and reports no leak.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeWaitsForRunningExpiry(CancellationToken cancellationToken)
    {
        var release = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var leaks = 0;
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var expiration = new ReplicaExpirationCoordinator<string>(PerKey((_, _) =>
        {
            _ = entered.TrySetResult();
            return release.Task;
        }))
        {
            ShutdownTimeProvider = new FakeTimeProvider(),
            ShutdownLeakReporter = _ => Interlocked.Increment(ref leaks),
        };
        var running = expiration.ExpireAsync("default", "key-a", cancellationToken);
        await entered.Task.WaitAsync(cancellationToken);

        var disposal = expiration.DisposeAsync().AsTask();
        _ = await Assert.That(disposal.IsCompleted).IsFalse();
        release.SetResult(null);
        await disposal.WaitAsync(cancellationToken);

        _ = await Assert.That(await running).IsNull();
        _ = await Assert.That(Volatile.Read(ref leaks)).IsEqualTo(0);
    }

    /// <summary>An expiry after dispose started is refused.</summary>
    [Test]
    public async Task ExpireAfterDisposeIsRefused()
    {
        var expiration = new ReplicaExpirationCoordinator<string>(PerKey(static (_, _) => Task.FromResult<string?>(null))) { ShutdownTimeProvider = TimeProvider.System };
        await expiration.DisposeAsync();

        var error = NodeExceptionAssert.For<ObjectDisposedException>().Throws(expiration, static disposed => _ = disposed.ExpireAsync("default", "key-a", CancellationToken.None));

        _ = await Assert.That(error).IsNotNull();
    }

    /// <summary>Operation ids are stable, domain-separated, lowercase 32-hex values.</summary>
    [Test]
    public async Task ExpirationIdIsStableAndSeparated()
    {
        var expiresUtc = new DateTime(638900000000000000, DateTimeKind.Utc);
        var first = ReplicaExpirationOperationId.Create("group-a", "default", "key-a", 7, expiresUtc);
        var repeated = ReplicaExpirationOperationId.Create("group-a", "default", "key-a", 7, expiresUtc);
        var boundary = ReplicaExpirationOperationId.Create("group-a", "defaul", "tkey-a", 7, expiresUtc);

        _ = await Assert.That(repeated).IsEqualTo(first);
        _ = await Assert.That(first).IsEqualTo("f6e3fa560b869c4cfa8a26062a016ee9");
        _ = await Assert.That(boundary).IsNotEqualTo(first, StringComparer.Ordinal);
        _ = await Assert.That(first.Length).IsEqualTo(32);
        _ = await Assert.That(first).Matches("^[0-9a-f]{32}$");
    }

    /// <summary>No cache can be named like the tombstone scope, so a client operation never shares it.</summary>
    [Test]
    public async Task ExpirationScopeIsNotCacheName()
    {
        var ex = NodeExceptionAssert.For<ArgumentException>().Throws(static () => _ = ServerCacheName.ParsePublic(ReplicaExpirationOperationId.OperationScope));

        _ = await Assert.That(ex.ParamName).IsEqualTo("cacheName");
    }

    /// <summary>Once an expiry of a key ends, the next expiry of that key runs again instead of reusing the finished one.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FinishedExpiryRunsAgain(CancellationToken cancellationToken)
    {
        var runs = 0;
        await using var expiration = new ReplicaExpirationCoordinator<string>(PerKey((_, _) => Task.FromResult(Interlocked.Increment(ref runs) == 1 ? "live" : null))) { ShutdownTimeProvider = TimeProvider.System };

        var first = await expiration.ExpireAsync("default", "key-a", cancellationToken);
        var second = await expiration.ExpireAsync("default", "key-a", cancellationToken);

        _ = await Assert.That(first).IsEqualTo("live");
        _ = await Assert.That(second).IsNull();
        _ = await Assert.That(Volatile.Read(ref runs)).IsEqualTo(2);
    }

    private static Func<IReadOnlyList<(string CacheName, string Key)>, Task<(string? Entry, Exception? Failure)[]>> PerKey(Func<string, string, Task<string?>> expire) =>
        async keys =>
        {
            var results = new (string? Entry, Exception? Failure)[keys.Count];
            for (var i = 0; i < keys.Count; i++)
                results[i] = (await expire(keys[i].CacheName, keys[i].Key).ConfigureAwait(false), null);

            return results;
        };

    /// <summary>Batch delegate whose first call parks until released; every call records its size, and a set refusal fails the first call.</summary>
    private sealed class BlockingBatches : IDisposable
    {
        private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal ConcurrentQueue<int> Sizes { get; } = new();

        internal Task Entered => _entered.Task;

        internal Exception? Refusal { get; init; }

        public void Dispose() => _ = _released.TrySetResult();

        internal async Task<(string? Entry, Exception? Failure)[]> RunAsync(IReadOnlyList<(string CacheName, string Key)> keys)
        {
            Sizes.Enqueue(keys.Count);
            if (Sizes.Count == 1)
            {
                _ = _entered.TrySetResult();
                await _released.Task.ConfigureAwait(false);
                if (Refusal != null)
                    throw Refusal;
            }

            return new (string? Entry, Exception? Failure)[keys.Count];
        }

        internal void Release() => _ = _released.TrySetResult();
    }
}
