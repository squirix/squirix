using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>Guards <see cref="IndexAllocator" /> against the concurrent-read rewind fixed in d3f6b8e20.</summary>
public sealed class IndexAllocatorTests : ServerUnitTestBase
{
    /// <summary>
    /// A cold seed (cache-misread on an uninitialized allocator) establishes the next index from the published
    /// index it read from disk, and later allocations continue monotonically from there.
    /// </summary>
    [Test]
    public async Task ColdSeedEstablishesNextIndex()
    {
        var allocator = new IndexAllocator("data", "current", "manifest", "manifest*", static () => null);
        allocator.SeedNextManifestIndex(10);
        _ = await Assert.That(allocator.AllocateNextManifestIndex()).IsEqualTo(11);
    }

    /// <summary>
    /// Two concurrent cold seeds must serialize through the double-checked guard: exactly one establishes the
    /// next index and the loser observes the inner guard and returns without overwriting. This exercises the
    /// inner <c language="csharp">if (_nextIndexInitialized) return;</c> branch that single-threaded seeds cannot reach.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentSeedTakesInnerDoubleCheckGuard(CancellationToken cancellationToken)
    {
        var allocator = new IndexAllocator("data", "current", "manifest", "manifest*", static () => null);

        var first = Task.Factory.StartNew(() => allocator.SeedNextManifestIndex(7), cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        var second = Task.Factory.StartNew(() => allocator.SeedNextManifestIndex(7), cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        await Task.WhenAll(first, second);

        _ = await Assert.That(allocator.AllocateNextManifestIndex()).IsEqualTo(8);
    }

    /// <summary>
    /// A cache-miss read (<see cref="IndexAllocator.SeedNextManifestIndex" />) that observes a stale, lower
    /// published index while the allocator was already initialized to a higher value must not rewind the next
    /// index. Without the guard this reseeds to the stale value and the following allocation reuses an index.
    /// </summary>
    [Test]
    public async Task StaleSeedNeverRewindsNextIndex()
    {
        var allocator = new IndexAllocator("data", "current", "manifest", "manifest*", static () => 15);
        _ = await Assert.That(allocator.AllocateNextManifestIndex()).IsEqualTo(16);
        allocator.SeedNextManifestIndex(10);
        _ = await Assert.That(allocator.AllocateNextManifestIndex()).IsEqualTo(17);
    }
}
