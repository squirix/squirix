using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.App.Decorators;

/// <summary>Covers local-owner journal logging paths introduced by the durable pipeline refactor.</summary>
[Immutable]
public sealed class JournalLoggingCacheDecoratorTests : ServerUnitTestBase
{
    private const string CacheName = "cache";
    private const string Remote = "node-b";
    private const string Self = "node-a";

    /// <summary>TryAdd skips the journal when the key already exists.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddSkipsJournalWhenKeyExists(CancellationToken cancellationToken)
    {
        await using var harness = await CreateHarnessAsync(Self, cancellationToken);
        _ = await Assert.That(await harness.Cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, "k", CreateEntry("v1"), cancellationToken)).IsTrue();
        var before = harness.Journal.AppendedOps;

        _ = await Assert.That(await harness.Cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, "k", CreateEntry("v2"), cancellationToken)).IsFalse();
        _ = await Assert.That(harness.Journal.AppendedOps).IsEqualTo(before);
    }

    /// <summary>JournalPayloadPrepareCacheDecorator.UpdateAsync delegates to the journal decorator for an existing key.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PayloadPrepareUpdateDelegatesToJournal(CancellationToken cancellationToken)
    {
        await using var harness = await CreateHarnessAsync(Self, cancellationToken);
        _ = await Assert.That(await harness.Cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, "k", CreateEntry("v1"), cancellationToken)).IsTrue();
        var prepare = new JournalPayloadPrepareCacheDecorator<string>(Self, RocksDoubles.CreateOwnerLocator(Self), harness.Cache);
        var before = harness.Journal.AppendedOps;

        _ = await Assert.That(await prepare.UpdateAsync(UnitMutationOpIds.Default, CacheName, "k", "v2", cancellationToken)).IsTrue();
        _ = await Assert.That(harness.Journal.AppendedOps).IsEqualTo(before + 1);
    }

    /// <summary>Non-local owners skip journal appends.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoteOwnerRemoveAppendsNoJournal(CancellationToken cancellationToken)
    {
        await using var harness = await CreateHarnessAsync(Remote, cancellationToken);
        var before = harness.Journal.AppendedOps;
        _ = await harness.Cache.RemoveAsync(UnitMutationOpIds.Default, CacheName, "k", cancellationToken);
        _ = await Assert.That(harness.Journal.AppendedOps).IsEqualTo(before);
        _ = await Assert.That(harness.Inner.RemoveCalls).IsEqualTo(1);
    }

    /// <summary>Local-owner remove appends a journal record then applies memory.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveAsyncLocalOwnerAppendsJournal(CancellationToken cancellationToken)
    {
        await using var harness = await CreateHarnessAsync(Self, cancellationToken);
        _ = await Assert.That(await harness.Cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, "k", CreateEntry("v"), cancellationToken)).IsTrue();
        var before = harness.Journal.AppendedOps;

        var removed = await harness.Cache.RemoveAsync(UnitMutationOpIds.Default, CacheName, "k", cancellationToken);

        _ = await Assert.That(removed.Removed).IsTrue();
        _ = await Assert.That(harness.Journal.AppendedOps).IsEqualTo(before + 1);
    }

    /// <summary>Local-owner set appends a put journal record.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetEntryAsyncLocalOwnerAppendsJournal(CancellationToken cancellationToken)
    {
        await using var harness = await CreateHarnessAsync(Self, cancellationToken);
        var before = harness.Journal.AppendedOps;

        await harness.Cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, "k", CreateEntry("v"), cancellationToken);

        _ = await Assert.That(harness.Journal.AppendedOps).IsEqualTo(before + 1);
        _ = await Assert.That(harness.Inner.SetCalls).IsEqualTo(1);
    }

    /// <summary>Local-owner touch appends one put record holding the touched entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchAsyncLocalOwnerAppendsJournal(CancellationToken cancellationToken)
    {
        await using var harness = await CreateHarnessAsync(Self, cancellationToken);
        _ = await Assert.That(await harness.Cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, "k", CreateEntry("v"), cancellationToken)).IsTrue();
        var before = harness.Journal.AppendedOps;

        _ = await Assert.That(await harness.Cache.TouchAsync(UnitMutationOpIds.Default, CacheName, "k", TimeSpan.FromMinutes(1), cancellationToken)).IsTrue();
        _ = await Assert.That(harness.Journal.AppendedOps).IsEqualTo(before + 1);

        var (operation, journaled) = ReadLastRecord(harness.Dir, cancellationToken);
        _ = await Assert.That(operation).IsEqualTo(JournalOperationKind.Put);
        _ = await Assert.That(journaled).IsNotNull();
        _ = await Assert.That(journaled!.Value).IsEqualTo("v");
        _ = await Assert.That(journaled.Version).IsEqualTo(1);
        _ = await Assert.That(journaled.ExpiresUtc > DateTime.UtcNow).IsTrue();
    }

    /// <summary>With a raw reader the decision read never goes through the inner cache read path, so only the memory apply counts as an access.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DecisionReadBypassesInnerRead(CancellationToken cancellationToken)
    {
        await using var harness = await CreateHarnessAsync(Self, cancellationToken, true);
        var withDeadline = new NodeCacheEntry<string>("v", expiresUtc: DateTime.UtcNow.AddHours(1));
        _ = await Assert.That(await harness.Cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, "k", withDeadline, cancellationToken)).IsTrue();

        _ = await Assert.That(await harness.Cache.TouchAsync(UnitMutationOpIds.Default, CacheName, "k", TimeSpan.FromHours(2), cancellationToken)).IsTrue();
        _ = await Assert.That(await harness.Cache.UpdateAsync(UnitMutationOpIds.Default, CacheName, "k", "w", cancellationToken)).IsTrue();
        _ = await Assert.That(await harness.Cache.RemoveExpirationAsync(UnitMutationOpIds.Default, CacheName, "k", cancellationToken)).IsTrue();

        var callsBeforeRead = harness.Inner.GetEntryCalls;
        var entry = await harness.Inner.GetEntryAsync(CacheName, "k", cancellationToken);
        _ = await Assert.That(callsBeforeRead).IsEqualTo(0);
        _ = await Assert.That(entry!.Value).IsEqualTo("w");
        _ = await Assert.That(entry.ExpiresUtc).IsNull();
    }

    /// <summary>Touch returns false without journaling when the key is missing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchMissingKeyAppendsNoJournal(CancellationToken cancellationToken)
    {
        await using var harness = await CreateHarnessAsync(Self, cancellationToken);
        var before = harness.Journal.AppendedOps;

        _ = await Assert.That(await harness.Cache.TouchAsync(UnitMutationOpIds.Default, CacheName, "missing", TimeSpan.FromMinutes(1), cancellationToken)).IsFalse();
        _ = await Assert.That(harness.Journal.AppendedOps).IsEqualTo(before);
    }

    /// <summary>Removing the expiration of an entry that has none returns false without journaling.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PersistWithoutDeadlineAppendsNoJournal(CancellationToken cancellationToken)
    {
        await using var harness = await CreateHarnessAsync(Self, cancellationToken);
        _ = await Assert.That(await harness.Cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, "k", CreateEntry("v"), cancellationToken)).IsTrue();
        var before = harness.Journal.AppendedOps;

        _ = await Assert.That(await harness.Cache.RemoveExpirationAsync(UnitMutationOpIds.Default, CacheName, "k", cancellationToken)).IsFalse();
        _ = await Assert.That(harness.Journal.AppendedOps).IsEqualTo(before);
    }

    /// <summary>Removing the expiration of a missing key returns false without journaling.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PersistMissingKeyAppendsNoJournal(CancellationToken cancellationToken)
    {
        await using var harness = await CreateHarnessAsync(Self, cancellationToken);
        var before = harness.Journal.AppendedOps;

        _ = await Assert.That(await harness.Cache.RemoveExpirationAsync(UnitMutationOpIds.Default, CacheName, "missing", cancellationToken)).IsFalse();
        _ = await Assert.That(harness.Journal.AppendedOps).IsEqualTo(before);
    }

    /// <summary>Removing the expiration of an entry with a deadline appends one put record and clears the deadline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PersistWithDeadlineAppendsJournal(CancellationToken cancellationToken)
    {
        await using var harness = await CreateHarnessAsync(Self, cancellationToken);
        var entry = new NodeCacheEntry<string>("v", expiresUtc: DateTime.UtcNow.AddHours(1));
        _ = await Assert.That(await harness.Cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, "k", entry, cancellationToken)).IsTrue();
        var before = harness.Journal.AppendedOps;

        _ = await Assert.That(await harness.Cache.RemoveExpirationAsync(UnitMutationOpIds.Default, CacheName, "k", cancellationToken)).IsTrue();
        _ = await Assert.That(harness.Journal.AppendedOps).IsEqualTo(before + 1);
        _ = await Assert.That((await harness.Inner.GetEntryAsync(CacheName, "k", cancellationToken))!.ExpiresUtc).IsNull();
    }

    /// <summary>Update on an existing local-owner key appends a put journal record and applies the memory update.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UpdateExistingKeyAppendsAndApplies(CancellationToken cancellationToken)
    {
        await using var harness = await CreateHarnessAsync(Self, cancellationToken);
        _ = await Assert.That(await harness.Cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, "k", CreateEntry("v1"), cancellationToken)).IsTrue();
        var before = harness.Journal.AppendedOps;

        _ = await Assert.That(await harness.Cache.UpdateAsync(UnitMutationOpIds.Default, CacheName, "k", "v2", cancellationToken)).IsTrue();
        _ = await Assert.That(harness.Journal.AppendedOps).IsEqualTo(before + 1);

        var updated = await harness.Inner.GetValueAsync(CacheName, "k", cancellationToken);
        _ = await Assert.That(updated.Found).IsTrue();
        _ = await Assert.That(updated.Value).IsEqualTo("v2");
    }

    /// <summary>Update returns false without journaling when the key is missing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UpdateMissingKeyAppendsNoJournal(CancellationToken cancellationToken)
    {
        await using var harness = await CreateHarnessAsync(Self, cancellationToken);
        var before = harness.Journal.AppendedOps;

        _ = await Assert.That(await harness.Cache.UpdateAsync(UnitMutationOpIds.Default, CacheName, "missing", "v", cancellationToken)).IsFalse();
        _ = await Assert.That(harness.Journal.AppendedOps).IsEqualTo(before);
    }

    /// <summary>Update skips the journal when the key vanishes between the public existence check and the durable apply, so replay cannot resurrect it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UpdateSkipsJournalWhenKeyVanishes(CancellationToken cancellationToken)
    {
        await using var harness = await CreateHarnessWithRaceInnerAsync(Self, cancellationToken);
        _ = await Assert.That(await harness.Cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, "k", CreateEntry("v1"), cancellationToken)).IsTrue();
        var before = harness.Journal.AppendedOps;

        _ = await Assert.That(await harness.Cache.UpdateAsync(UnitMutationOpIds.Default, CacheName, "k", "v2", cancellationToken)).IsFalse();
        _ = await Assert.That(harness.Journal.AppendedOps).IsEqualTo(before);
    }

    private static (JournalOperationKind Operation, NodeCacheEntry<string>? Entry) ReadLastRecord(string dir, CancellationToken cancellationToken)
    {
        NodeCacheEntry<string>? none = null;
        var last = (Operation: JournalOperationKind.WaitForStartup, Entry: none);
        using var records = JournalReadPath.ReadAll(dir, 1, cancellationToken);
        while (records.MoveNext())
        {
            // The reader reuses its frame buffer, so the entry is decoded before the next record is read.
            _ = JournalEntryPayload.TryDecode<string>(records.Current.PutEntryBytes.Span, out var entry);
            last = (records.Current.Operation, entry);
        }

        return last;
    }

    private static NodeCacheEntry<string> CreateEntry(string value) => new() { Value = value, Version = 1 };

    /// <summary>Creates a journal-logging decorator harness for the given owner.</summary>
    /// <param name="owner">The cache owner.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <param name="useRawReader">Whether the decorator reads the decision entry through the physical cache raw reader.</param>
    private static async Task<Harness> CreateHarnessAsync(string owner, CancellationToken cancellationToken, bool useRawReader = false)
    {
        var dir = new TempDirectory("squirix-journal-logging-decorator");
        var options = new PersistenceOptions
        {
            DataDir = dir,
            JournalMaxSegmentMb = 1,
            FlushInterval = 5,
            ManifestRetentionCount = 1,
        };
        var manifestStore = new Ledger(options, NullLogger<Ledger>.Instance);
        var journal = JournalCoordinatorFactory.Create(options, await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken), manifestStore, new AsyncManualResetEvent(true), NullLoggerFactory.Instance, out _);
        var physical = new PhysicalCache<string>();
        var inner = new RecordingLogicalCache(physical);
        var executor = new DurableMutationExecutor(journal, NullLogger<DurableMutationExecutor>.Instance);
        var rawReader = useRawReader ? physical.RawReader : null;
        var cache = new JournalLoggingCacheDecorator<string>(Self, RocksDoubles.CreateOwnerLocator(owner), inner, journal, executor, null, rawReader);
        return new Harness(dir, manifestStore, journal, inner, cache);
    }

    /// <summary>Creates a journal-logging decorator harness with a race-simulating inner cache.</summary>
    /// <param name="owner">The cache owner.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    private static async Task<Harness> CreateHarnessWithRaceInnerAsync(string owner, CancellationToken cancellationToken)
    {
        var dir = new TempDirectory("squirix-journal-logging-decorator");
        var options = new PersistenceOptions
        {
            DataDir = dir,
            JournalMaxSegmentMb = 1,
            FlushInterval = 5,
            ManifestRetentionCount = 1,
        };
        var manifestStore = new Ledger(options, NullLogger<Ledger>.Instance);
        var journal = JournalCoordinatorFactory.Create(options, await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken), manifestStore, new AsyncManualResetEvent(true), NullLoggerFactory.Instance, out _);
        var physical = new PhysicalCache<string>();
        var inner = new RaceSimulatingInnerCache(physical);
        var executor = new DurableMutationExecutor(journal, NullLogger<DurableMutationExecutor>.Instance);
        var cache = new JournalLoggingCacheDecorator<string>(Self, RocksDoubles.CreateOwnerLocator(owner), inner, journal, executor);
        return new Harness(dir, manifestStore, journal, inner, cache);
    }

    [Immutable]
    private sealed class Harness : IAsyncDisposable
    {
        private readonly TempDirectory _dir;
        private readonly Ledger _manifestStore;

        internal Harness(TempDirectory dir, Ledger manifestStore, IJournalCoordinator journal, RecordingLogicalCache inner, JournalLoggingCacheDecorator<string> cache)
        {
            _dir = dir;
            _manifestStore = manifestStore;
            Journal = journal;
            Inner = inner;
            Cache = cache;
        }

        internal JournalLoggingCacheDecorator<string> Cache { get; }

        internal string Dir => _dir;

        internal RecordingLogicalCache Inner { get; }

        internal IJournalCoordinator Journal { get; }

        public async ValueTask DisposeAsync()
        {
            await Journal.DisposeAsync();
            _manifestStore.Dispose();
            _dir.Dispose();
        }
    }

    private sealed class RaceSimulatingInnerCache : RecordingLogicalCache
    {
        internal RaceSimulatingInnerCache(PhysicalCache<string> physical)
            : base(physical)
        {
        }

        public override ValueTask<NodeCacheEntry<string>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult<NodeCacheEntry<string>?>(null);

        public override ValueTask<NodeCacheValueResult<string>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new NodeCacheValueResult<string>(false, null));
    }

    private class RecordingLogicalCache : ILogicalNamespacedCache<string>
    {
        private readonly ClientCache<string> _inner;

        internal RecordingLogicalCache(PhysicalCache<string> physical)
        {
            _inner = new ClientCache<string>(physical, physical);
        }

        internal int GetEntryCalls { get; private set; }

        internal int RemoveCalls { get; private set; }

        internal int SetCalls { get; private set; }

        public virtual ValueTask<NodeCacheEntry<string>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken)
        {
            GetEntryCalls++;
            return _inner.GetEntryAsync(cacheName, key, cancellationToken);
        }

        public virtual ValueTask<NodeCacheValueResult<string>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            _inner.GetValueAsync(cacheName, key, cancellationToken);

        public ValueTask<CacheRemoveResult<string>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
        {
            RemoveCalls++;
            return _inner.RemoveAsync(operationId, cacheName, key, cancellationToken);
        }

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken);

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken)
        {
            SetCalls++;
            return _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken);
        }

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
            _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken);

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken) =>
            _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken);

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, string? value, CancellationToken cancellationToken) =>
            _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken);
    }
}
