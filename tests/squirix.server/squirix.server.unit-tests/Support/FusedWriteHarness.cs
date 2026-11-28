using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Adapters.Wire;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Endpoint;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Runtime.Invocation;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Logging;
using Squirix.Server.Threading;
using Squirix.Testing;
using Squirix.Transport.Wire.Cache;
using TUnit.Mocks;
using TUnit.Mocks.Arguments;
using TUnit.Mocks.Generated;

namespace Squirix.Server.UnitTests.Support;

/// <summary>
/// The hosted write path of a single node over a real journal on a stallable segment writer: the binary adapter, the
/// journal decorator and memory, all on one fake clock. The journal belongs to the caller.
/// </summary>
[Mutable]
internal sealed class FusedWriteHarness
{
    /// <summary>Name of the cache every request targets.</summary>
    internal const string CacheName = "c";

    /// <summary>Key every request targets unless it names another.</summary>
    internal const string Key = "k";

    private const string Self = "node-a";

    internal FusedWriteHarness(
        string dir,
        StallableJournal journal,
        ILogicalNamespacedCache<StoredValue>? innerOverride = null,
        Func<JournalLoggingCacheDecorator<StoredValue>, ILogicalNamespacedCache<StoredValue>>? chain = null)
    {
        Dir = dir;
        Journal = journal;
        Clock = JournalReplayKit.CreateWriteClock(TimeSpan.Zero);
        Physical = new PhysicalCache<StoredValue>(Clock, new CacheEntrySizeEstimator<StoredValue>());
        Executor = new DurableMutationExecutor(journal.Journal, NullLogger<DurableMutationExecutor>.Instance);
        Cache = new JournalLoggingCacheDecorator<StoredValue>(innerOverride ?? new ClientCache<StoredValue>(Physical, Physical), journal.Journal, Executor, Clock, Physical.RawReader);
        Adapter = CreateAdapter(chain?.Invoke(Cache) ?? Cache);
    }

    /// <summary>Gets the adapter whose handlers use the journal.</summary>
    internal RoutedCacheOperations<StoredValue> Adapter { get; }

    /// <summary>Gets the journal decorator the handlers write through.</summary>
    internal JournalLoggingCacheDecorator<StoredValue> Cache { get; }

    /// <summary>Gets the clock of the decorator, the memory and the adapter.</summary>
    internal FakeTimeProvider Clock { get; }

    /// <summary>Gets the data directory of the journal.</summary>
    internal string Dir { get; }

    /// <summary>Gets the number of journal flushes completed so far.</summary>
    internal long FlushCount => Journal.Journal.FlushCount;

    /// <summary>Gets the memory the decorator applies to.</summary>
    internal PhysicalCache<StoredValue> Physical { get; }

    /// <summary>Gets the journal over the stallable writer.</summary>
    private StallableJournal Journal { get; }

    private DurableMutationExecutor Executor { get; }

    /// <summary>Builds a GetOrAdd request.</summary>
    /// <param name="operationId">Operation id.</param>
    /// <param name="value">The value added when the key is absent.</param>
    /// <param name="key">Cache key.</param>
    /// <returns>The request.</returns>
    internal static GetOrAddAsyncRequest GetOrAdd(string operationId, string value, string key = Key) =>
        new() { CacheName = CacheName, Key = key, OperationId = operationId, Entry = Entry(value, TimeSpan.FromMinutes(1)) };

    /// <summary>Builds a Remove request.</summary>
    /// <param name="operationId">Operation id.</param>
    /// <param name="key">Cache key.</param>
    /// <returns>The request.</returns>
    internal static RemoveAsyncRequest Remove(string operationId, string key = Key) => new() { CacheName = CacheName, Key = key, OperationId = operationId };

    /// <summary>Builds a RemoveExpiration request.</summary>
    /// <param name="operationId">Operation id.</param>
    /// <param name="key">Cache key.</param>
    /// <returns>The request.</returns>
    internal static RemoveExpirationAsyncRequest RemoveExpiration(string operationId, string key = Key) => new() { CacheName = CacheName, Key = key, OperationId = operationId };

    /// <summary>Builds a SetEntry request.</summary>
    /// <param name="operationId">Operation id.</param>
    /// <param name="value">The value.</param>
    /// <param name="key">Cache key.</param>
    /// <param name="ttl">The relative expiration, or <see langword="null" /> for none.</param>
    /// <returns>The request.</returns>
    internal static SetEntryAsyncRequest Set(string operationId, string value = "v", string key = Key, TimeSpan? ttl = null) =>
        new() { CacheName = CacheName, Key = key, OperationId = operationId, Entry = Entry(value, ttl) };

    /// <summary>Builds a Touch request.</summary>
    /// <param name="operationId">Operation id.</param>
    /// <param name="expiration">The new relative expiration.</param>
    /// <param name="key">Cache key.</param>
    /// <returns>The request.</returns>
    internal static TouchAsyncRequest Touch(string operationId, TimeSpan expiration, string key = Key) =>
        new() { CacheName = CacheName, Key = key, OperationId = operationId, Expiration = expiration };

    /// <summary>Builds a TryAddEntry request.</summary>
    /// <param name="operationId">Operation id.</param>
    /// <param name="value">The value added when the key is absent.</param>
    /// <param name="key">Cache key.</param>
    /// <returns>The request.</returns>
    internal static TryAddEntryAsyncRequest AddIfAbsent(string operationId, string value, string key = Key) =>
        new() { CacheName = CacheName, Key = key, OperationId = operationId, Entry = Entry(value, TimeSpan.FromMinutes(1)) };

    /// <summary>Builds an Update request.</summary>
    /// <param name="operationId">Operation id.</param>
    /// <param name="value">The new value.</param>
    /// <param name="key">Cache key.</param>
    /// <returns>The request.</returns>
    internal static UpdateAsyncRequest Update(string operationId, string value, string key = Key) =>
        new() { CacheName = CacheName, Key = key, OperationId = operationId, Entry = Entry(value) };

    /// <summary>Builds a distinct valid operation id.</summary>
    /// <param name="number">The distinguishing number.</param>
    /// <returns>A 32-character lowercase hex operation id.</returns>
    internal static string OpId(int number) => number.ToString("x32", System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>Reads every frame the segments hold right now, in order.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The frames.</returns>
    internal List<Frame> ReadFrames(CancellationToken cancellationToken)
    {
        var frames = new List<Frame>();
        using var records = JournalReadPath.ReadAll(Dir, 1, cancellationToken);
        while (records.MoveNext())
        {
            var record = records.Current;
            frames.Add(new Frame(record.Sequence, record.Operation, record.MutationOperationId, record.IdempotencyOperationId, record.IdempotencyResponseBytes.ToArray()));
        }

        return frames;
    }

    /// <summary>Recovers the journal into fresh cache memory; the journal must be shut down first.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The recovered memory.</returns>
    internal async Task<PhysicalCache<StoredValue>> RecoverAsync(CancellationToken cancellationToken)
    {
        var persistence = new PersistenceOptions { DataDir = Dir, JournalMaxSegmentMb = 1 };
        using var ledger = new Ledger(persistence, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance);
        var memory = new PhysicalCache<StoredValue>(Clock, new CacheEntrySizeEstimator<StoredValue>());
        var dependencies = new RecoveryDependencies<StoredValue>(persistence, ledger, memory, new AsyncManualResetEvent(true), StoreFactory.CreateReader(), Clock);
        await new RecoveryService<StoredValue>(new RecoveryOptions { BlockOnStart = true }, NullLogger<RecoveryService<StoredValue>>.Instance, dependencies).StartAsync(cancellationToken);
        return memory;
    }

    /// <summary>Creates the wire form of an entry holding a string.</summary>
    /// <param name="value">The value.</param>
    /// <param name="ttl">The relative expiration, or <see langword="null" /> for none.</param>
    /// <returns>The wire entry.</returns>
    private static CacheEntryWire Entry(string value, TimeSpan? ttl = null) => new()
    {
        Value = JsonValueKit.Wire("{\"__v\":\"" + value + "\"}"),
        Expiration = ttl,
    };

    private RoutedCacheOperations<StoredValue> CreateAdapter(ILogicalNamespacedCache<StoredValue> api)
    {
        var operations = IInboundEndpointCacheOperations<StoredValue>.Mock(MockBehavior.Strict);
        _ = operations.ForCache(CacheName).Returns(new RoutedCacheApi<StoredValue>(api, CacheName));
        var ownership = INodeOwnershipResolver.Mock(MockBehavior.Strict);
        _ = ownership.SelfNodeId.Returns(Self);
        _ = ownership.GetOwner(Arg.Any<string>(), Arg.Any<string>()).Returns(Self);
        var invocation = IRemoteInvocationState.Mock(MockBehavior.Strict);
        _ = invocation.IsInternalOwnerInvocation.Returns(false);

        // Nothing is forwarded: the pool and the backpressure gate have no setups.
        return new RoutedCacheOperations<StoredValue>(
            operations.Object,
            OwnerRouters.Static(ownership.Object, invocation.Object, Self),
            new OwnerRpcForwarder(
                IServerClientPool.Mock(MockBehavior.Strict).Object,
                IBackpressureGate.Mock(MockBehavior.Strict).Object,
                IBackpressureClientIdResolver.Mock(MockBehavior.Strict).Object,
                RingAgreements.Create()),
            false,
            Clock,
            journal: Journal.Journal);
    }

    /// <summary>A journal frame as the segments hold it.</summary>
    /// <param name="Sequence">Journal sequence.</param>
    /// <param name="Operation">Operation kind.</param>
    /// <param name="MutationOperationId">Operation id a mutation frame is stamped with.</param>
    /// <param name="OutcomeOperationId">Operation id an outcome frame belongs to.</param>
    /// <param name="OutcomeBytes">Response bytes of an outcome frame.</param>
    [Immutable]
    internal readonly record struct Frame(ulong Sequence, JournalOperationKind Operation, string? MutationOperationId, string? OutcomeOperationId, byte[] OutcomeBytes);
}
