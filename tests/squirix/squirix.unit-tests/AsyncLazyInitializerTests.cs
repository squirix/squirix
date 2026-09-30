using System;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Internal.Threading;
using Squirix.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

#pragma warning disable VSTHRD003 // Handing one task to every caller is the behavior under test; the gates are completion sources the tests own.

/// <summary>Single-run coordination of <see cref="AsyncLazyInitializer" />.</summary>
[Immutable]
public sealed class AsyncLazyInitializerTests : UnitTestBase
{
    /// <summary>Canceled work leaves the shared task canceled rather than faulted.</summary>
    [Test]
    public async Task CancellationCancelsSharedTask()
    {
        var owner = new Owner(static _ => throw new OperationCanceledException());

        var task = owner.StartAsync();

        _ = await AsyncAssert.ThrowsAnyAsync<OperationCanceledException, bool>(AwaitAsync(task));
        _ = await Assert.That(task.IsCanceled).IsTrue();
    }

    /// <summary>A second call while the work is running gets the running task and does not complete before it.</summary>
    [Test]
    public async Task ConcurrentCallWaitsForRunningWork()
    {
        var owner = new Owner(static o => o.Gate.Task);

        var first = owner.StartAsync();
        var second = owner.StartAsync();
        _ = await Assert.That(second).IsSameReferenceAs(first);
        _ = await Assert.That(second.IsCompleted).IsFalse();

        owner.Gate.SetResult();
        await second;
        _ = await Assert.That(owner.Runs).IsEqualTo(1);
    }

    /// <summary>A failure of the single run reaches every caller.</summary>
    [Test]
    public async Task FailureReachesEveryCaller()
    {
        var owner = new Owner(static async o =>
        {
            await o.Gate.Task;
            throw new InvalidOperationException("Work failed.");
        });

        var first = owner.StartAsync();
        var second = owner.StartAsync();
        owner.Gate.SetResult();

        var firstFailure = await AsyncAssert.ThrowsAsync<InvalidOperationException, bool>(AwaitAsync(first));
        var secondFailure = await AsyncAssert.ThrowsAsync<InvalidOperationException, bool>(AwaitAsync(second));
        _ = await Assert.That(secondFailure).IsSameReferenceAs(firstFailure);
    }

    /// <summary>A call made from inside the work gets the task of the run in progress instead of starting a second run.</summary>
    [Test]
    public async Task ReentrantCallGetsSameTask()
    {
        var owner = new Owner(static o =>
        {
            o.Inner = o.StartAsync();
            return Task.CompletedTask;
        });

        var outer = owner.StartAsync();
        await outer;

        _ = await Assert.That(ReferenceEquals(owner.Inner, outer)).IsTrue();
        _ = await Assert.That(owner.Runs).IsEqualTo(1);
    }

    /// <summary>Work that throws before returning a task faults the shared task instead of leaving it pending.</summary>
    [Test]
    public async Task SynchronousThrowFaultsSharedTask()
    {
        var owner = new Owner(static _ => throw new InvalidOperationException("Work failed."));

        _ = await AsyncAssert.ThrowsAsync<InvalidOperationException, bool>(AwaitAsync(owner.StartAsync()));
    }

    private static async ValueTask<bool> AwaitAsync(Task task)
    {
        await task.ConfigureAwait(false);
        return true;
    }

    /// <summary>Holds the task slot, as the owner of a single run does.</summary>
    private sealed class Owner
    {
        private readonly Func<Owner, Task> _work;
        private Task? _slot;

        internal Owner(Func<Owner, Task> work)
        {
            _work = work;
        }

        internal TaskCompletionSource Gate { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Task? Inner { get; set; }

        internal int Runs { get; private set; }

        internal Task StartAsync() => AsyncLazyInitializer.EnsureStartedAsync(ref _slot, this, static owner => owner.RunWorkAsync());

        private Task RunWorkAsync()
        {
            Runs++;
            return _work(this);
        }
    }
}
