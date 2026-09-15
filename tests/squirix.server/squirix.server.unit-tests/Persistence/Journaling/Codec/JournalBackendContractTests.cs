using System;
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
using Squirix.Server.TestKit.IO;
using Squirix.Server.Threading;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Codec;

/// <summary>Contract tests for the pipelined journal coordinator.</summary>
[Immutable]
public sealed class JournalBackendContractTests
{
    /// <summary>Append and replay round-trip for the pipelined journal backend.</summary>
    [Test]
    public async Task AppendPutReplayRoundTripAsync()
    {
        await using var context = await CreateCoordinatorAsync();
        var key = new CacheKey("ns", "k1");
        var payload = JournalEntryPayloadKit.EncodePut(1);
        await context.Coordinator.AppendPutAsync(key, payload, CancellationToken.None);
        await context.Coordinator.AwaitDurabilityCommitAsync(CancellationToken.None);

        var last = await ReadLastRecordAsync(context);
        _ = await Assert.That(last.Operation).IsEqualTo(JournalOperationKind.Put);
        _ = await Assert.That(last.Key.Key).IsEqualTo(key.Key);
    }

    /// <summary>Append remove and replay round-trip for the pipelined journal backend.</summary>
    [Test]
    public async Task AppendRemoveReplayRoundTripAsync()
    {
        await using var context = await CreateCoordinatorAsync();
        var key = new CacheKey("ns", "remove-key");
        await context.Coordinator.AppendRemoveAsync(key, CancellationToken.None);
        await context.Coordinator.AwaitDurabilityCommitAsync(CancellationToken.None);

        var last = await ReadLastRecordAsync(context);
        _ = await Assert.That(last.Operation).IsEqualTo(JournalOperationKind.Remove);
        _ = await Assert.That(last.Key.Namespace).IsEqualTo(key.Namespace);
        _ = await Assert.That(last.Key.Key).IsEqualTo(key.Key);
    }

    /// <summary>Append remove-expiration and replay round-trip for the pipelined journal backend.</summary>
    [Test]
    public async Task RemoveExpiryReplayRoundTripAsync()
    {
        await using var context = await CreateCoordinatorAsync();
        var key = new CacheKey("ns", "remove-exp-key");
        await context.Coordinator.AppendRemoveExpirationAsync(key, CancellationToken.None);
        await context.Coordinator.AwaitDurabilityCommitAsync(CancellationToken.None);

        var last = await ReadLastRecordAsync(context);
        _ = await Assert.That(last.Operation).IsEqualTo(JournalOperationKind.RemoveExpiration);
        _ = await Assert.That(last.Key.Namespace).IsEqualTo(key.Namespace);
        _ = await Assert.That(last.Key.Key).IsEqualTo(key.Key);
    }

    /// <summary>Append touch-expiration and replay round-trip for the pipelined journal backend.</summary>
    [Test]
    public async Task TouchExpiryReplayRoundTripAsync()
    {
        await using var context = await CreateCoordinatorAsync();
        var key = new CacheKey("ns", "touch-key");
        var expiresUtc = new DateTime(2026, 6, 30, 12, 0, 0, DateTimeKind.Utc);
        await context.Coordinator.AppendTouchExpirationAsync(key, expiresUtc, CancellationToken.None);
        await context.Coordinator.AwaitDurabilityCommitAsync(CancellationToken.None);

        var last = await ReadLastRecordAsync(context);
        _ = await Assert.That(last.Operation).IsEqualTo(JournalOperationKind.TouchExpiration);
        _ = await Assert.That(last.Key.Key).IsEqualTo(key.Key);
        _ = await Assert.That(last.TouchExpirationUtc).IsEqualTo(expiresUtc);
    }

    private static async Task<CoordinatorContext> CreateCoordinatorAsync()
    {
        var dir = new TempDirectory("journal-contract");
        var options = new PersistenceOptions { DataDir = dir, JournalMaxSegmentMb = 64 };
        var manifestStore = new Ledger(options);
        var gate = new AsyncManualResetEvent(true);
        var manifest = await manifestStore.ReadCurrentOrDefaultAsync(CancellationToken.None);
        var coordinator = JournalCoordinatorFactory.Create(options, manifest, manifestStore, gate);
        return new CoordinatorContext(dir, options, manifestStore, coordinator);
    }

    private static async Task<JournalRecord> ReadLastRecordAsync(CoordinatorContext context)
    {
        var manifest = await context.Ledger.ReadCurrentOrDefaultAsync(CancellationToken.None);
        JournalRecord? last = null;
        using var records = JournalReadPath.ReadAll(context.Options.DataDir, manifest.CurrentJournal, CancellationToken.None);
        while (records.MoveNext())
            last = records.Current;

        _ = await Assert.That(last).IsNotNull();
        return last!;
    }

    [Immutable]
    private sealed class CoordinatorContext : IAsyncDisposable
    {
        private readonly TempDirectory _directory;

        internal CoordinatorContext(TempDirectory directory, PersistenceOptions options, Ledger manifestStore, IJournalCoordinator coordinator)
        {
            _directory = directory;
            Options = options;
            Ledger = manifestStore;
            Coordinator = coordinator;
        }

        internal IJournalCoordinator Coordinator { get; }

        internal Ledger Ledger { get; }

        internal PersistenceOptions Options { get; }

        public async ValueTask DisposeAsync()
        {
            await Coordinator.DisposeAsync().ConfigureAwait(false);
            Ledger.Dispose();
            _directory.Dispose();
        }
    }
}
