using System.Threading.Tasks;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>Guards <see cref="IndexAllocator" /> against the concurrent-read rewind fixed in d3f6b8e20.</summary>
public sealed class IndexAllocatorTests : ServerUnitTestBase
{
    /// <summary>
    /// A cold seed (cache-miss read on an uninitialized allocator) establishes the next index from the published
    /// index it read from disk, and subsequent allocations continue monotonically from there.
    /// </summary>
    [Fact]
    public void ColdSeedEstablishesNextIndex()
    {
        var allocator = new IndexAllocator("data", "current", "manifest", "manifest*", static () => null);
        allocator.SeedNextManifestIndex(10);
        Assert.Equal(11, allocator.AllocateNextManifestIndex());
    }

    /// <summary>
    ///     Two concurrent cold seeds must serialize through the double-checked guard: exactly one establishes the
    ///     next index and the loser observes the inner guard and returns without overwriting. This exercises the
    ///     inner <c language="csharp">if (_nextIndexInitialized) return;</c> branch that single-threaded seeds cannot reach.
    /// </summary>
    [Fact]
    public async Task ConcurrentSeedTakesInnerDoubleCheckGuard()
    {
        var allocator = new IndexAllocator("data", "current", "manifest", "manifest*", static () => null);

        var first = Task.Factory.StartNew(() => allocator.SeedNextManifestIndex(7), DefaultCancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        var second = Task.Factory.StartNew(() => allocator.SeedNextManifestIndex(7), DefaultCancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        await Task.WhenAll(first, second);

        Assert.Equal(8, allocator.AllocateNextManifestIndex());
    }

    /// <summary>
    /// A cache-miss read (<see cref="IndexAllocator.SeedNextManifestIndex" />) that observes a stale, lower
    /// published index while the allocator was already initialized to a higher value must not rewind the next
    /// index. Without the guard this reseeds to the stale value and the following allocation reuses an index.
    /// </summary>
    [Fact]
    public void StaleSeedNeverRewindsNextIndex()
    {
        var allocator = new IndexAllocator("data", "current", "manifest", "manifest*", static () => 15);
        Assert.Equal(16, allocator.AllocateNextManifestIndex());
        allocator.SeedNextManifestIndex(10);
        Assert.Equal(17, allocator.AllocateNextManifestIndex());
    }
}
