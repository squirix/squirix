using System.Threading;
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
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Covers on-disk journal byte accounting for newly created segment headers.</summary>
[Immutable]
public sealed class JournalBootstrapHeaderAccountingTests : ServerUnitTestBase
{
    private static readonly byte[] SamplePayload = [1, 2, 3];

    /// <summary>First append on a fresh journal includes the segment file header in UsedBytes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FirstAppendCountsFileHeaderInUsedBytes(CancellationToken cancellationToken)
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
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));

        await journal.AppendPutAndAwaitDurabilityAsync(new CacheKey(ServerCacheNames.DefaultNamespace, "k"), SamplePayload, cancellationToken);

        _ = await Assert.That(journal.UsedBytes >= JournalFraming.FileHeaderSize).IsTrue();
        _ = await Assert.That(journal.UsedBytes > JournalFraming.FileHeaderSize).IsTrue();
    }
}
