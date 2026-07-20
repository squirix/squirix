using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Node.App.Decorators;

/// <summary>Covers local-owner journal logging paths introduced by the durable pipeline refactor.</summary>
public sealed class JournalLoggingCacheDecoratorTests : ServerUnitTestBase
{
    private const string CacheName = "cache";
    private const string Self = "node-a";
    private const string Remote = "node-b";

    /// <summary>Non-local owners skip journal appends.</summary>
    [Fact]
    public async Task RemoveAsyncRemoteOwnerDoesNotAppendJournal()
    {
        await using var harness = await CreateHarnessAsync(Remote);
        var before = harness.Journal.AppendedOps;
        _ = await harness.Cache.RemoveAsync(UnitMutationOpIds.Default, CacheName, "k", DefaultCancellationToken);
        Assert.Equal(before, harness.Journal.AppendedOps);
        Assert.Equal(1, harness.Inner.RemoveCalls);
    }

    /// <summary>Local-owner remove appends a journal record then applies memory.</summary>
    [Fact]
    public async Task RemoveAsyncLocalOwnerAppendsJournal()
    {
        await using var harness = await CreateHarnessAsync(Self);
        Assert.True(await harness.Cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, "k", CreateEntry("v"), DefaultCancellationToken));
        var before = harness.Journal.AppendedOps;

        var removed = await harness.Cache.RemoveAsync(UnitMutationOpIds.Default, CacheName, "k", DefaultCancellationToken);

        Assert.True(removed.Removed);
        Assert.Equal(before + 1, harness.Journal.AppendedOps);
    }

    /// <summary>Local-owner touch appends a journal record.</summary>
    [Fact]
    public async Task TouchAsyncLocalOwnerAppendsJournal()
    {
        await using var harness = await CreateHarnessAsync(Self);
        Assert.True(await harness.Cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, "k", CreateEntry("v"), DefaultCancellationToken));
        var before = harness.Journal.AppendedOps;

        Assert.True(await harness.Cache.TouchAsync(UnitMutationOpIds.Default, CacheName, "k", TimeSpan.FromMinutes(1), DefaultCancellationToken));
        Assert.Equal(before + 1, harness.Journal.AppendedOps);
    }

    /// <summary>TryAdd skips journal when the key already exists.</summary>
    [Fact]
    public async Task TryAddEntryAsyncSkipsJournalWhenKeyExists()
    {
        await using var harness = await CreateHarnessAsync(Self);
        Assert.True(await harness.Cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, "k", CreateEntry("v1"), DefaultCancellationToken));
        var before = harness.Journal.AppendedOps;

        Assert.False(await harness.Cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, "k", CreateEntry("v2"), DefaultCancellationToken));
        Assert.Equal(before, harness.Journal.AppendedOps);
    }

    /// <summary>Update returns false without journaling when the key is missing.</summary>
    [Fact]
    public async Task UpdateAsyncMissingKeyDoesNotAppendJournal()
    {
        await using var harness = await CreateHarnessAsync(Self);
        var before = harness.Journal.AppendedOps;

        Assert.False(await harness.Cache.UpdateAsync(UnitMutationOpIds.Default, CacheName, "missing", "v", DefaultCancellationToken));
        Assert.Equal(before, harness.Journal.AppendedOps);
    }

    /// <summary>Local-owner set appends a put journal record.</summary>
    [Fact]
    public async Task SetEntryAsyncLocalOwnerAppendsJournal()
    {
        await using var harness = await CreateHarnessAsync(Self);
        var before = harness.Journal.AppendedOps;

        await harness.Cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, "k", CreateEntry("v"), DefaultCancellationToken);

        Assert.Equal(before + 1, harness.Journal.AppendedOps);
        Assert.Equal(1, harness.Inner.SetCalls);
    }

    private static NodeCacheEntry<string> CreateEntry(string value) => new() { Value = value, Version = 1 };

    private static async Task<Harness> CreateHarnessAsync(string owner)
    {
        var dir = new TempDirectory("squirix-journal-logging-decorator");
        var options = new PersistenceOptions
        {
            DataDir = dir,
            JournalMaxSegmentMb = 1,
            FlushIntervalMs = 5,
            ManifestRetentionCount = 1,
        };
        var manifestStore = new ManifestStore(options);
        var journal = await JournalCoordinatorFactory.CreateAsync(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);
        var physical = new PhysicalCache<string>();
        var inner = new RecordingLogicalCache(physical);
        var executor = new DurableMutationExecutor(journal);
        var cache = new JournalLoggingCacheDecorator<string>(Self, new FixedOwnerLocator(owner), inner, journal, executor);
        return new Harness(dir, manifestStore, journal, physical, inner, cache);
    }

    private sealed class Harness : IAsyncDisposable
    {
        private readonly TempDirectory _dir;
        private readonly ManifestStore _manifestStore;
        private readonly PhysicalCache<string> _physical;

        internal Harness(
            TempDirectory dir,
            ManifestStore manifestStore,
            IJournalCoordinator journal,
            PhysicalCache<string> physical,
            RecordingLogicalCache inner,
            JournalLoggingCacheDecorator<string> cache)
        {
            _dir = dir;
            _manifestStore = manifestStore;
            Journal = journal;
            _physical = physical;
            Inner = inner;
            Cache = cache;
        }

        internal JournalLoggingCacheDecorator<string> Cache { get; }

        internal RecordingLogicalCache Inner { get; }

        internal IJournalCoordinator Journal { get; }

        public async ValueTask DisposeAsync()
        {
            await Journal.DisposeAsync();
            await _physical.DisposeAsync();
            _manifestStore.Dispose();
            _dir.Dispose();
        }
    }

    private sealed class FixedOwnerLocator : INodeLocator
    {
        private readonly string _owner;

        internal FixedOwnerLocator(string owner) => _owner = owner;

        public string GetOwner(string cacheName, string key) => _owner;
    }

    private sealed class RecordingLogicalCache : ILogicalNamespacedCache<string>
    {
        private readonly ClientCache<string> _inner;

        internal RecordingLogicalCache(PhysicalCache<string> physical) => _inner = new ClientCache<string>(physical, physical);

        internal int RemoveCalls { get; private set; }

        internal int SetCalls { get; private set; }

        public ValueTask<NodeCacheEntry<string>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            _inner.GetEntryAsync(cacheName, key, cancellationToken);

        public ValueTask<NodeCacheValueResult<string>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            _inner.GetValueAsync(cacheName, key, cancellationToken);

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken)
        {
            SetCalls++;
            return _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken);
        }

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
            _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken);

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken) =>
            _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken);

        public ValueTask<CacheRemoveResult<string>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken)
        {
            RemoveCalls++;
            return _inner.RemoveAsync(operationId, cacheName, key, cancellationToken);
        }

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken);

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, string? value, CancellationToken cancellationToken) =>
            _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken);
    }
}
