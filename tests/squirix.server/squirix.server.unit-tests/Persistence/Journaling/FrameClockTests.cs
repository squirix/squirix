using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Journal frames are stamped with the server clock the journal runs on.</summary>
[Immutable]
public sealed class FrameClockTests : IsolatedStorageTestBase
{
    /// <summary>A frame written under a fake clock carries the fake clock's time, not the host's.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FrameCarriesServerClockTime(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var persistence = new PersistenceOptions { DataDir = Dir };
        using var store = new Ledger(persistence, NullLogger<Ledger>.Instance);
        await using (var journal = JournalCoordinatorFactory.Create(
            persistence,
            await store.ReadCurrentOrDefaultAsync(cancellationToken),
            store,
            new AsyncManualResetEvent(true),
            NullLoggerFactory.Instance,
            clock,
            out _))
        {
            await journal.AppendPutUnderGateAsync(CacheKey.Default("k"), JournalEntryPayloadKit.EncodePut("v"), cancellationToken);
            await journal.AwaitDurabilityCommitAsync(cancellationToken);
        }

        long? stamped = null;
        using var records = JournalReadPath.ReadAll(Dir, 1, cancellationToken);
        while (records.MoveNext())
            stamped = records.Current.UnixMs;

        _ = await Assert.That(stamped).IsEqualTo(clock.GetUtcNow().ToUnixTimeMilliseconds());
    }
}
