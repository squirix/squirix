using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Snapshot;

/// <summary>The windowed snapshot loader returns exactly what the record-by-record reader returned.</summary>
[Immutable]
public sealed class LoadEquivalenceTests : IsolatedStorageTestBase
{
    private const int SmallEntryCount = 300;

    /// <summary>An empty snapshot loads to nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EmptySnapshotLoadsEmpty(CancellationToken cancellationToken)
    {
        var path = await new SnapshotWriter(Dir).WriteAsync(1, [], [], cancellationToken);

        var loaded = await StoreFactory.CreateReader(16).LoadStrictAsync<object?>(path, null, cancellationToken);

        _ = await Assert.That(loaded.Entries.Count).IsEqualTo(0);
        _ = await Assert.That(loaded.IdempotencyRecords.Count).IsEqualTo(0);
    }

    /// <summary>Expired entries are skipped only when a cut-off is given.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredEntriesAreSkippedOnlyWithCutoff(CancellationToken cancellationToken)
    {
        var path = await WriteMixedAsync(cancellationToken);
        var reader = StoreFactory.CreateReader(37);

        var all = await reader.LoadStrictAsync<object?>(path, null, cancellationToken);
        var live = await reader.LoadStrictAsync<object?>(path, LoadSupport.LoadUtc, cancellationToken);

        _ = await Assert.That(all.Entries.Count).IsEqualTo(SmallEntryCount + 5);
        _ = await Assert.That(live.Entries.Count).IsLessThan(all.Entries.Count);
        _ = await Assert.That(live.EncodedLengths.Count).IsEqualTo(live.Entries.Count);
        _ = await Assert.That(all.IdempotencyRecords.Count).IsEqualTo(2);
    }

    /// <summary>Entries, lengths and idempotency records equal the oracle for windows that split records at many offsets.</summary>
    /// <param name="windowSize">The window size under test.</param>
    /// <param name="expiredCutoff">Whether expired entries are skipped.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(16, false)]
    [Arguments(16, true)]
    [Arguments(37, false)]
    [Arguments(37, true)]
    [Arguments(1000, true)]
    [Arguments(4096, false)]
    [Arguments(65_536, true)]
    [Arguments(524_288, false)]
    [Arguments(524_288, true)]
    public async Task LoadedContentEqualsOracle(int windowSize, bool expiredCutoff, CancellationToken cancellationToken)
    {
        var path = await WriteMixedAsync(cancellationToken);
        DateTime? expiredAsOf = expiredCutoff ? LoadSupport.LoadUtc : null;

        var expected = await LoadSupport.DescribeLoadAsync(new LegacyReader(), path, expiredAsOf, cancellationToken);
        var actual = await LoadSupport.DescribeLoadAsync(StoreFactory.CreateReader(windowSize), path, expiredAsOf, cancellationToken);

        _ = await Assert.That(expected.StartsWith("OK|", StringComparison.Ordinal)).IsTrue();
        _ = await Assert.That(actual).IsEqualTo(expected);
    }

    private async Task<string> WriteMixedAsync(CancellationToken cancellationToken)
    {
        var writer = new SnapshotWriter(Dir);
        return await writer.WriteAsync(writer.AllocateIndex(0), LoadSupport.BuildItems(SmallEntryCount, LoadSupport.LargeValueLength), LoadSupport.BuildIdempotency(), cancellationToken);
    }
}
