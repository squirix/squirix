using System;
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

#pragma warning disable VSTHRD003 // One expiry run is handed to every caller of a key, which is the behavior under test; the gates are completion sources the tests own.

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
        await using var expiration = new ReplicaExpirationCoordinator<string>((_, _) =>
        {
            _ = Interlocked.Increment(ref runs);
            return release.Task;
        });
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
        await using var expiration = new ReplicaExpirationCoordinator<string>(static (_, _) => Task.FromResult(CallerScope.Value));
        CallerScope.Value = "caller";

        var seen = await expiration.ExpireAsync("default", "key-a", cancellationToken);

        _ = await Assert.That(seen).IsNull();
    }

    /// <summary>Concurrent expiries of one key share a single run; another key runs on its own.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentExpiriesShareOneRun(CancellationToken cancellationToken)
    {
        var release = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var runs = 0;
        await using var expiration = new ReplicaExpirationCoordinator<string>((cacheName, key) =>
        {
            _ = Interlocked.Increment(ref runs);
            return string.Equals(cacheName + key, "defaultkey-a", StringComparison.Ordinal) ? release.Task : Task.FromResult<string?>(null);
        });

        var first = expiration.ExpireAsync("default", "key-a", cancellationToken);
        var second = expiration.ExpireAsync("default", "key-a", cancellationToken);
        var other = await expiration.ExpireAsync("default", "key-b", cancellationToken);
        release.SetResult("live");

        _ = await Assert.That(await first).IsEqualTo("live");
        _ = await Assert.That(await second).IsEqualTo("live");
        _ = await Assert.That(other).IsNull();
        _ = await Assert.That(Volatile.Read(ref runs)).IsEqualTo(2);
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
        var expiration = new ReplicaExpirationCoordinator<string>((_, _) => never.Task)
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
        var expiration = new ReplicaExpirationCoordinator<string>((_, _) => release.Task)
        {
            ShutdownTimeProvider = new FakeTimeProvider(),
            ShutdownLeakReporter = _ => Interlocked.Increment(ref leaks),
        };
        var running = expiration.ExpireAsync("default", "key-a", cancellationToken);

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
        var expiration = new ReplicaExpirationCoordinator<string>(static (_, _) => Task.FromResult<string?>(null));
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
        await using var expiration = new ReplicaExpirationCoordinator<string>((_, _) => Task.FromResult(Interlocked.Increment(ref runs) == 1 ? "live" : null));

        var first = await expiration.ExpireAsync("default", "key-a", cancellationToken);
        var second = await expiration.ExpireAsync("default", "key-a", cancellationToken);

        _ = await Assert.That(first).IsEqualTo("live");
        _ = await Assert.That(second).IsNull();
        _ = await Assert.That(Volatile.Read(ref runs)).IsEqualTo(2);
    }
}
