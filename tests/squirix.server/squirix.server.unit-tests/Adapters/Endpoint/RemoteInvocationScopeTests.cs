using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Runtime.Invocation;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Endpoint;

/// <summary>Public and internal invocation classification is restored across nested and asynchronous calls.</summary>
[Immutable]
public sealed class RemoteInvocationScopeTests : ServerUnitTestBase
{
    /// <summary>An ordinary public invocation stays public before, inside, and after its scope.</summary>
    [Test]
    public async Task PublicScopeKeepsPublicClassification()
    {
        _ = await Assert.That(RemoteInvocationContext.IsInternalOwnerInvocation).IsFalse();
        using (RemoteInvocationContext.EnterRemoteInvocation())
            _ = await Assert.That(RemoteInvocationContext.IsInternalOwnerInvocation).IsFalse();
        _ = await Assert.That(RemoteInvocationContext.IsInternalOwnerInvocation).IsFalse();
    }

    /// <summary>A public call nested inside an owner call still checks the payload limit.</summary>
    [Test]
    public async Task NestedPublicScopeEnforcesPayloadLimit()
    {
        var physical = new PhysicalCache<object?>(TimeProvider.System, new ObjectCacheEntrySizeEstimator());
        var cache = new ValidationCacheDecorator<object?>(new ClientCache<object?>(physical, physical), 3);
        var entry = new NodeCacheEntry<object?>(new StoredText([0xe7, 0x95, 0x8c, 0x78]));

        using var owner = RemoteInvocationContext.EnterRemoteInvocation(true);
        using (RemoteInvocationContext.EnterRemoteInvocation())
        {
            var error = NodeExceptionAssert.For<SquirixException>().Throws(
                (cache, entry),
                static state => _ = state.cache.SetEntryAsync("op", "default", "key", state.entry, CancellationToken.None).AsTask());

            _ = await Assert.That(error.Code).IsEqualTo(SquirixErrorCode.PayloadTooLarge);
        }

        await cache.SetEntryAsync("op", "default", "key", entry, CancellationToken.None);
        var stored = await cache.GetValueAsync("default", "key", CancellationToken.None);
        _ = await Assert.That(stored.Found).IsTrue();
        _ = await Assert.That(stored.RawValue).IsSameReferenceAs(entry.RawValue);
    }

    /// <summary>A public distributed write can use the full payload budget with an expiration envelope.</summary>
    [Test]
    public async Task PublicDistributedWriteAtLimit()
    {
        var physical = new PhysicalCache<object?>(TimeProvider.System, new ObjectCacheEntrySizeEstimator());
        var cache = new ValidationCacheDecorator<object?>(new ClientCache<object?>(physical, physical), 3);
        var policy = new DistributedCachePolicy(null, null);
        var write = new DistributedCacheWrite(in policy, [1, 2, 3], TimeSpan.FromMinutes(1).Ticks);

        using var scope = RemoteInvocationContext.EnterRemoteInvocation();
        await cache.SetEntryAsync("op", "default", "key", new NodeCacheEntry<object?>(write), CancellationToken.None);

        var stored = await cache.GetValueAsync("default", "key", CancellationToken.None);
        _ = await Assert.That(stored.RawValue).IsSameReferenceAs(write.Envelope);
    }

    /// <summary>A public invocation nested inside an owner call clears the owner flag, then restores it.</summary>
    [Test]
    public async Task NestedPublicRestoresOwner()
    {
        using (RemoteInvocationContext.EnterRemoteInvocation(true))
        {
            using (RemoteInvocationContext.EnterRemoteInvocation())
                _ = await Assert.That(RemoteInvocationContext.IsInternalOwnerInvocation).IsFalse();
            _ = await Assert.That(RemoteInvocationContext.IsInternalOwnerInvocation).IsTrue();
        }

        _ = await Assert.That(RemoteInvocationContext.IsInternalOwnerInvocation).IsFalse();
    }

    /// <summary>A same-value nested owner scope does not erase its parent classification.</summary>
    [Test]
    public async Task NestedOwnerRestoresOwner()
    {
        using (RemoteInvocationContext.EnterRemoteInvocation(true))
        {
            using (RemoteInvocationContext.EnterRemoteInvocation(true))
                _ = await Assert.That(RemoteInvocationContext.IsInternalOwnerInvocation).IsTrue();
            _ = await Assert.That(RemoteInvocationContext.IsInternalOwnerInvocation).IsTrue();
        }

        _ = await Assert.That(RemoteInvocationContext.IsInternalOwnerInvocation).IsFalse();
    }

    /// <summary>Both classifications flow across asynchronous suspension and restore the enclosing scope.</summary>
    /// <param name="internalOwner">Whether the invocation is an internal owner call.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ClassificationFlowsAcrossAwait(bool internalOwner)
    {
        using (RemoteInvocationContext.EnterRemoteInvocation(internalOwner))
        {
            await Task.Yield();
            _ = await Assert.That(RemoteInvocationContext.IsInternalOwnerInvocation).IsEqualTo(internalOwner);
        }

        _ = await Assert.That(RemoteInvocationContext.IsInternalOwnerInvocation).IsFalse();
    }

    /// <summary>A child invocation can change classification without changing its caller's context.</summary>
    [Test]
    public async Task ChildDoesNotChangeParent()
    {
        using var parent = RemoteInvocationContext.EnterRemoteInvocation(true);
        _ = await Assert.That(await ReadAfterYieldAsync(false)).IsFalse();
        _ = await Assert.That(RemoteInvocationContext.IsInternalOwnerInvocation).IsTrue();
    }

    /// <summary>Overlapping public and owner calls retain separate classifications until both complete.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentCallsAreIsolated(CancellationToken cancellationToken)
    {
        var ownerReady = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var publicReady = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var owner = ReadOverlappingAsync(true, ownerReady, publicReady.Task, cancellationToken);
        var ordinary = ReadOverlappingAsync(false, publicReady, ownerReady.Task, cancellationToken);

        _ = await Assert.That(await owner).IsTrue();
        _ = await Assert.That(await ordinary).IsFalse();
        _ = await Assert.That(RemoteInvocationContext.IsInternalOwnerInvocation).IsFalse();
    }

    private static async Task<bool> ReadAfterYieldAsync(bool internalOwner)
    {
        using var scope = RemoteInvocationContext.EnterRemoteInvocation(internalOwner);
        await Task.Yield();
        return RemoteInvocationContext.IsInternalOwnerInvocation;
    }

    private static async Task<bool> ReadOverlappingAsync(bool internalOwner, TaskCompletionSource ready, Task peerReady, CancellationToken cancellationToken)
    {
        using var scope = RemoteInvocationContext.EnterRemoteInvocation(internalOwner);
        ready.SetResult();
        await peerReady.WaitAsync(cancellationToken);
        await Task.Yield();
        return RemoteInvocationContext.IsInternalOwnerInvocation;
    }
}
