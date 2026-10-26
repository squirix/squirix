using System;
using System.Diagnostics.Metrics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Google.Protobuf;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>
/// Allocation cost per entry of what a follower does with a state transfer message: parsing the message of the wire, and writing its entries through the real
/// state sink, the journal chain and the cache of the node.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3, invocationCount: 16)]
public class ReplicaStateTransferApplyBenchmarks
{
    private const string CacheName = "cache";
    private const int EntriesPerMessage = 64;
    private const string GroupId = "n1";
    private const int MessagesPerInvoke = 16;
    private const int ValueChars = 4096;
    private static readonly byte[] Fingerprint = [9, 8, 7];
    private ReplicaGroupCommitters? _committers;
    private TempDirectory? _dir;
    private ReplicaStateEntry[] _entries = [];
    private JournalBenchmarkHost? _host;
    private Meter? _meter;
    private byte[] _message = [];
    private ReplicaGroupRegistry? _registry;
    private ReplicaStateSink? _sink;

    /// <summary>Disposes the journal, the registry and the data directory.</summary>
    /// <returns>A task that completes when cleanup finishes.</returns>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        if (_committers != null)
            await _committers.DisposeAsync().ConfigureAwait(false);
        if (_registry != null)
            await _registry.DisposeAsync().ConfigureAwait(false);
        if (_host != null)
            await _host.DisposeAsync().ConfigureAwait(false);
        _meter?.Dispose();
        _dir?.Dispose();
        _sink = null;
        _committers = null;
        _registry = null;
        _host = null;
        _meter = null;
        _dir = null;
    }

    /// <summary>Parses a message of entries as the transport hands it to the receiver.</summary>
    /// <remarks>
    /// Most of the transfer garbage on a follower is here: Google.Protobuf copies every payload into a new <see cref="ByteString" />, about one value size per entry,
    /// because the transport buffer is valid only during deserialization. Removing it needs a custom server marshaller for the transfer call that parses entries
    /// into pooled buffers and releases them after the handler.
    /// </remarks>
    /// <returns>The number of entries parsed.</returns>
    [Benchmark(OperationsPerInvoke = MessagesPerInvoke * EntriesPerMessage)]
    public int ParseMessage()
    {
        var parsed = 0;
        for (var i = 0; i < MessagesPerInvoke; i++)
            parsed += TransferReplicaStateRequest.Parser.ParseFrom(_message).Entries.Entries.Count;

        return parsed;
    }

    /// <summary>Creates the sink over a real journal, cache and group log, and the entries and the message to apply.</summary>
    /// <returns>A task that completes when setup finishes.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        _dir = new TempDirectory("replica-transfer-apply-bench");
        await SeededLogs.SeedGroupsAsync(_dir, [GroupId], CancellationToken.None).ConfigureAwait(false);
        var logOptions = new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System };
        _registry = new ReplicaGroupRegistry(_dir, [GroupId], 3, Fingerprint, 1, NullLoggerFactory.Instance, logOptions) { ElectionClock = TimeProvider.System };
        await _registry.OpenAsync(CancellationToken.None).ConfigureAwait(false);

        _host = await JournalBenchmarkHost.CreateAsync("replica-transfer-apply-journal", new PersistenceOptions { JournalMaxSegmentMb = 64 }, CancellationToken.None)
            .ConfigureAwait(false);
        var physical = new PhysicalCache<object?>(TimeProvider.System, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
        var client = new ClientCache<object?>(physical, physical);
        var logging = new JournalLoggingCacheDecorator<object?>(
            client,
            _host.Coordinator,
            new DurableMutationExecutor(_host.Coordinator, NullLogger<DurableMutationExecutor>.Instance),
            TimeProvider.System,
            physical.RawReader);
        var local = new JournalPayloadPrepareCacheDecorator<object?>(logging);
        _meter = new Meter("replica-transfer-apply-bench");
        var appliers = new ReplicaGroupAppliers(_registry, local, GroupId, NullLogger<ReplicaGroupAppliers>.Instance, new ReplicationMetrics(_meter));
        _committers = new ReplicaGroupCommitters([], "n9", new SingleGroupLocator(), TimeProvider.System);
        _sink = new ReplicaStateSink(appliers, _committers, local, new ReplicaGroupEntries(physical, new SingleGroupLocator()), _host.Coordinator, new HealthySnapshots());

        _entries = new ReplicaStateEntry[EntriesPerMessage];
        var wire = new ReplicaTransferEntries();
        for (var i = 0; i < _entries.Length; i++)
        {
            var key = Encoding.UTF8.GetBytes($"key-{i:D6}");
            var payload = ReplicaCacheApplier.EncodeEntry(new NodeCacheEntry<object?>(new string('x', ValueChars)));
            _entries[i] = new ReplicaStateEntry(CacheName, key, payload, 0);
            wire.Entries.Add(new ReplicaTransferEntry { CacheName = CacheName, Key = ByteString.CopyFrom(key), Payload = ByteString.CopyFrom(payload) });
        }

        _message = new TransferReplicaStateRequest { TransferId = "t", Sequence = 1, Entries = wire }.ToByteArray();
    }

    /// <summary>Writes a message of entries through the sink, the journal chain and the cache.</summary>
    /// <returns>A task that completes when all entries are written.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = MessagesPerInvoke * EntriesPerMessage)]
    public async Task WriteMessageAsync()
    {
        var sink = ThrowHelper.Required(_sink, "Benchmark sink was not initialized.");
        _ = await sink.EnterAsync(GroupId, CancellationToken.None).ConfigureAwait(false);
        for (var i = 0; i < MessagesPerInvoke; i++)
        {
            var written = await sink.WriteAsync(GroupId, _entries, CancellationToken.None).ConfigureAwait(false);
            if (written != _entries.Length)
                throw new InvalidOperationException("The sink stopped before the batch was written.");
        }

        sink.Leave(GroupId);
    }

    /// <summary>Assigns every key to the group of the benchmark.</summary>
    [Immutable]
    private sealed class SingleGroupLocator : INodeLocator
    {
        public string GetOwner(string cacheName, string key) => GroupId;
    }

    /// <summary>Snapshot status that never failed.</summary>
    [Immutable]
    private sealed class HealthySnapshots : ISnapshotReadinessStatus
    {
        public SnapshotFailure? CurrentFailure => null;

        public bool HasFatalFailure => false;
    }
}
