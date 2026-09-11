using System;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Threading;

/// <summary>Verifies the async lock refuses acquisition after disposal.</summary>
public sealed class AsyncLockTests : ServerUnitTestBase
{
    /// <summary>Locking a disposed lock faults with ObjectDisposedException instead of waiting on a semaphore that no longer exists.</summary>
    [Fact]
    public async Task LockAfterDisposeThrowsObjectDisposed()
    {
        var asyncLock = new AsyncLock();
        asyncLock.Dispose();

        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException, AsyncLockHolder>(asyncLock.LockAsync(DefaultCancellationToken));
    }

    /// <summary>Try-locking a disposed lock throws ObjectDisposedException instead of touching a semaphore that no longer exists.</summary>
    [Fact]
    public void DisposedTryLockThrows()
    {
        var asyncLock = new AsyncLock();
        asyncLock.Dispose();

        _ = NodeExceptionAssert.For<ObjectDisposedException>().Throws(
            asyncLock,
            DefaultCancellationToken,
            static (candidate, token) => { _ = candidate.TryLock(out _, token); });
    }
}
