using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>
/// Disposing the coordinator must drain every append already enqueued to the journal ring before
/// the journal thread stops, so an abrupt shutdown never silently drops acknowledged mutations.
/// </summary>
[Immutable]
public sealed class JournalDisposeDrainTests : IsolatedStorageTestBase
{
    /// <summary>Appends enqueued right before disposal are all present in the journal after reopen.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposePersistsEnqueuedAppends(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 4,
            FlushInterval = 600_000,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        await journal.WaitForStartupAsync(cancellationToken);

        // Fire-and-forget: no durability waits, disposal must still persist every frame.
        for (var i = 0; i < 8; i++)
        {
            var key = CacheKey.Default($"drain{i}");
            await journal.AppendPutAsync(key, JournalEntryPayloadKit.EncodePut("v"), cancellationToken);
        }

        // Dispose explicitly before reading: shutdown must drain the ring. The trailing await-using
        // disposal is a no-op (dispose is idempotent).
        // ReSharper disable once DisposeOnUsingVariable
        await journal.DisposeAsync();

        var expectedKeys = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < 8; i++)
            _ = expectedKeys.Add($"drain{i}");

        var foundKeys = new HashSet<string>(StringComparer.Ordinal);
        using var records = JournalReadPath.ReadAll(Dir, 1, cancellationToken);
        while (records.MoveNext())
        {
            _ = await Assert.That(records.Current.Operation).IsEqualTo(JournalOperationKind.Put);
            _ = foundKeys.Add(records.Current.Key.Key);
        }

        _ = await Assert.That(expectedKeys.SetEquals(foundKeys)).IsTrue().Because($"expected keys: {string.Join(", ", expectedKeys)}; found keys: {string.Join(", ", foundKeys)}");
    }
}
