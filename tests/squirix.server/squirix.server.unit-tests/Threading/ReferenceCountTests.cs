using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Threading;

/// <summary>Unit tests for <see cref="ReferenceCount" /> retain and release semantics.</summary>
public sealed class ReferenceCountTests : ServerUnitTestBase
{
    /// <summary>Concurrent retains and releases that pair up leave the initial reference as the last one.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentRetainsAndReleasesBalance(CancellationToken cancellationToken)
    {
        var count = new ReferenceCount();
        await Parallel.ForEachAsync(
            [0, 1, 2, 3, 4, 5, 6, 7],
            cancellationToken,
            (_, _) =>
            {
                for (var n = 0; n < 10_000; n++)
                {
                    _ = count.TryRetain();
                    _ = count.Release();
                }

                return ValueTask.CompletedTask;
            });
        _ = await Assert.That(count.IsReleased).IsFalse();
        _ = await Assert.That(count.Release()).IsTrue();
    }

    /// <summary>A new count holds one reference that is not released yet.</summary>
    [Test]
    public async Task NewCountHoldsOneReference()
    {
        var count = new ReferenceCount();

        _ = await Assert.That(count.IsReleased).IsFalse();
    }

    /// <summary>Releasing more references than were held fails and leaves the count released.</summary>
    [Test]
    public async Task ReleaseBeyondZeroThrows()
    {
        var count = new ReferenceCount();
        _ = count.Release();

        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(count, static c => _ = c.Release());
        _ = await Assert.That(count.IsReleased).IsTrue();
    }

    /// <summary>Only the release that reaches zero reports it.</summary>
    [Test]
    public async Task ReleaseReachesZeroExactlyOnce()
    {
        var count = new ReferenceCount();
        _ = await Assert.That(count.TryRetain()).IsTrue();

        _ = await Assert.That(count.Release()).IsFalse();
        _ = await Assert.That(count.Release()).IsTrue();
        _ = await Assert.That(count.IsReleased).IsTrue();
    }

    /// <summary>A count that reached zero refuses further retains.</summary>
    [Test]
    public async Task RetainAfterZeroFails()
    {
        var count = new ReferenceCount();
        _ = count.Release();

        _ = await Assert.That(count.TryRetain()).IsFalse();
        _ = await Assert.That(count.IsReleased).IsTrue();
    }

    /// <summary>A count created with zero references is already released.</summary>
    [Test]
    public async Task ZeroInitialCountIsReleased()
    {
        var count = new ReferenceCount(0);

        _ = await Assert.That(count.IsReleased).IsTrue();
        _ = await Assert.That(count.TryRetain()).IsFalse();
    }
}
