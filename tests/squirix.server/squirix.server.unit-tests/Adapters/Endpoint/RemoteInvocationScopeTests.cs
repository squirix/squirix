using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Runtime.Invocation;
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
