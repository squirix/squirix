using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Endpoint;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Runtime.Invocation;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.Threading;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.UnitTests.Support;

/// <summary>
/// The hosted write path of a single node over a real journal on a stallable segment writer: the gRPC adapter, the idempotency coordinator, the
/// journal decorator and memory, all on one fake clock. The journal belongs to the caller, which disposes it; the meter is shared and lives for the test run.
/// </summary>
[Mutable]
internal sealed class FusedWriteHarness
{
    /// <summary>Name of the cache every request targets.</summary>
    internal const string CacheName = "c";

    /// <summary>Key every request targets unless it names another.</summary>
    internal const string Key = "k";

    private const string Self = "node-a";

    private static readonly Meter SharedMeter = new("fused-write-harness");

    internal FusedWriteHarness(
        string dir,
        StallableJournal journal,
        ILogicalNamespacedCache<object?>? innerOverride = null,
        Func<JournalLoggingCacheDecorator<object?>, ILogicalNamespacedCache<object?>>? chain = null)
    {
        Dir = dir;
        Journal = journal;
        Clock = JournalReplayKit.CreateWriteClock(TimeSpan.Zero);
        Physical = new PhysicalCache<object?>(Clock);
        Executor = new DurableMutationExecutor(journal.Journal, NullLogger<DurableMutationExecutor>.Instance);
        Cache = new JournalLoggingCacheDecorator<object?>(innerOverride ?? new ClientCache<object?>(Physical, Physical), journal.Journal, Executor, Clock, Physical.RawReader);
        Store = new RpcMutationIdempotencyStore(new IdempotencyOptions(), Self, new IdempotencyMetrics(SharedMeter), Clock);
        Coordinator = new RpcMutationIdempotencyCoordinator(Store, journal.Journal, NullLogger<RpcMutationIdempotencyCoordinator>.Instance);
        Adapter = CreateAdapter(chain?.Invoke(Cache) ?? Cache);
    }

    /// <summary>Gets the adapter whose handlers run under the coordinator.</summary>
    internal SquirixServiceAdapter<object?> Adapter { get; }

    /// <summary>Gets the journal decorator the handlers write through.</summary>
    internal JournalLoggingCacheDecorator<object?> Cache { get; }

    /// <summary>Gets the clock of the decorator, the memory, the store and the adapter.</summary>
    internal FakeTimeProvider Clock { get; }

    /// <summary>Gets the idempotency coordinator the adapter executes through.</summary>
    internal RpcMutationIdempotencyCoordinator Coordinator { get; }

    /// <summary>Gets the data directory of the journal.</summary>
    internal string Dir { get; }

    /// <summary>Gets the number of journal flushes completed so far.</summary>
    internal long FlushCount => Journal.Journal.FlushCount;

    /// <summary>Gets the journal over the stallable writer.</summary>
    internal StallableJournal Journal { get; }

    /// <summary>Gets the memory the decorator applies to.</summary>
    internal PhysicalCache<object?> Physical { get; }

    /// <summary>Gets the idempotency store.</summary>
    internal RpcMutationIdempotencyStore Store { get; }

    private DurableMutationExecutor Executor { get; }

    /// <summary>Creates the wire form of an entry holding a string.</summary>
    /// <param name="value">The value.</param>
    /// <param name="ttl">The relative expiration, or <see langword="null" /> for none.</param>
    /// <returns>The wire entry.</returns>
    internal static CacheEntryWire Entry(string value, TimeSpan? ttl = null) => new()
    {
        Value = Struct.Parser.ParseJson("{\"__v\":\"" + value + "\"}"),
        Expiration = ttl is { } span ? Duration.FromTimeSpan(span) : null,
    };

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
        new() { CacheName = CacheName, Key = key, OperationId = operationId, Expiration = Duration.FromTimeSpan(expiration) };

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

    /// <summary>Runs one idempotent set of the shared key under the coordinator, with the outcome projection a set handler registers.</summary>
    /// <param name="operationId">Operation id.</param>
    /// <param name="value">The value.</param>
    /// <param name="cancellationToken">Cancellation token of the write.</param>
    /// <returns>The response of the write.</returns>
    internal Task<SetAsyncResponse> SetThroughScopeAsync(string operationId, string value, CancellationToken cancellationToken) => Coordinator.ExecuteAsync(
        operationId,
        "fp",
        (Harness: this, OperationId: operationId, Value: value),
        static async (state, token) =>
        {
            RpcMutationIdempotencyExecutionAmbient.RegisterOutcomeProjection<bool>(static _ => new SetAsyncResponse());
            await state.Harness.Cache.SetEntryAsync(state.OperationId, CacheName, Key, new NodeCacheEntry<object?>(state.Value), token);
            return new SetAsyncResponse();
        },
        cancellationToken);

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

    /// <summary>Recovers the journal into a fresh memory and idempotency store, as a restart would; the journal must be shut down first.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The recovered memory and store.</returns>
    internal async Task<(PhysicalCache<object?> Memory, RpcMutationIdempotencyStore Store)> RecoverAsync(CancellationToken cancellationToken)
    {
        var persistence = new PersistenceOptions { DataDir = Dir, ManifestRetentionCount = 1, JournalMaxSegmentMb = 1 };
        using var ledger = new Ledger(persistence, NullLogger<Ledger>.Instance);
        var memory = new PhysicalCache<object?>(Clock);
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions(), Self, new IdempotencyMetrics(SharedMeter), Clock);
        var dependencies = new RecoveryDependencies<object?>(persistence, ledger, memory, new AsyncManualResetEvent(true), store, StoreFactory.CreateReader(), Clock);
        await new RecoveryService<object?>(new RecoveryOptions { BlockOnStart = true }, NullLogger<RecoveryService<object?>>.Instance, dependencies).StartAsync(cancellationToken);
        return (memory, store);
    }

    private SquirixServiceAdapter<object?> CreateAdapter(ILogicalNamespacedCache<object?> api)
    {
        var operations = new IGrpcCacheOperationsCreateExpectations<object?>();
        _ = operations.Setups.ForCache(CacheName).ReturnValue(new RoutedCacheApi<object?>(api, CacheName));
        var ownership = new INodeOwnershipResolverCreateExpectations();
        _ = ownership.Setups.SelfNodeId.Gets().ReturnValue(Self);
        _ = ownership.Setups.GetOwner(Arg.Any<string>(), Arg.Any<string>()).ReturnValue(Self);
        var invocation = new IRemoteInvocationStateCreateExpectations();
        _ = invocation.Setups.IsInternalOwnerInvocation.Gets().ReturnValue(false);

        // Nothing is forwarded: the pool and the backpressure gate have no setups.
        return new SquirixServiceAdapter<object?>(
            operations.Instance(),
            OwnerRouters.Static(ownership.Instance(), invocation.Instance(), Self),
            new OwnerRpcForwarder(
                new IServerClientPoolCreateExpectations().Instance(),
                new IBackpressureGateCreateExpectations().Instance(),
                new IBackpressureClientIdResolverCreateExpectations().Instance(),
                RingAgreements.Create()),
            Coordinator,
            Clock);
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
