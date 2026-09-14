using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Covers on-disk journal byte accounting for newly created segment headers.</summary>
[Immutable]
public sealed class JournalBootstrapHeaderAccountingTests : ServerUnitTestBase
{
    private static readonly byte[] SamplePayload = [1, 2, 3];

    /// <summary>First append on a fresh journal includes the segment file header in UsedBytes.</summary>
    [Fact]
    public async Task FirstAppendCountsFileHeaderInUsedBytes()
    {
        using var dir = new TempDirectory("squirix-journal-header-bytes");
        var options = new PersistenceOptions
        {
            DataDir = dir,
            JournalMaxSegmentMb = 1,
            JournalMaxTotalBytesMb = 1,
            FlushInterval = 5,
            ManifestRetentionCount = 1,
        };
        using var manifestStore = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));

        await journal.AppendPutAndAwaitDurabilityAsync(new CacheKey(ServerCacheNames.DefaultNamespace, "k"), SamplePayload, DefaultCancellationToken);

        Assert.True(journal.UsedBytes >= JournalFraming.FileHeaderSize);
        Assert.True(journal.UsedBytes > JournalFraming.FileHeaderSize);
    }
}
