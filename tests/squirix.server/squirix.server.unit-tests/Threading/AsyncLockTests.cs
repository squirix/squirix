using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Threading;

/// <summary>Verifies the async lock refuses acquisition after disposal.</summary>
public sealed class AsyncLockTests : ServerUnitTestBase
{
    /// <summary>Try-locking a disposed lock throws ObjectDisposedException instead of touching a semaphore that no longer exists.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public void DisposedTryLockThrows(CancellationToken cancellationToken)
    {
        var asyncLock = new AsyncLock();
        asyncLock.Dispose();

        _ = NodeExceptionAssert.For<ObjectDisposedException>().Throws(asyncLock, cancellationToken, static (candidate, token) => { _ = candidate.TryLock(out _, token); });
    }

    /// <summary>Releasing a holder after its lock was disposed under it (a bounded shutdown gave up waiting) is a no-op instead of a throw.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HolderReleaseAfterLockDisposeIsSafe(CancellationToken cancellationToken)
    {
        var asyncLock = new AsyncLock();
        var holder = await asyncLock.LockAsync(cancellationToken);
        asyncLock.Dispose();

        holder.Dispose();
    }

    /// <summary>Locking a disposed lock faults with ObjectDisposedException instead of waiting on a semaphore that no longer exists.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LockAfterDisposeThrowsObjectDisposed(CancellationToken cancellationToken)
    {
        var asyncLock = new AsyncLock();
        asyncLock.Dispose();

        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException, AsyncLockHolder>(asyncLock.LockAsync(cancellationToken));
    }
}
