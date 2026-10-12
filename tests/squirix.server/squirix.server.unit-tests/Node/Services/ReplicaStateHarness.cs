using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>A receiver of state transfers over a real group log, a real cache and the real sink, with a journal double that records what the metadata held at each barrier.</summary>
internal sealed class ReplicaStateHarness : IAsyncDisposable
{
    /// <summary>The group the harness serves.</summary>
    internal const string GroupId = TransferFixtures.GroupId;

    /// <summary>The leader that sends the transfer.</summary>
    internal const string LeaderId = "n2";

    /// <summary>The cache the entries of the tests belong to.</summary>
    internal const string CacheName = "cache";

    private readonly List<ReplicaTransferHeader> _begun = [];
    private ReplicaGroupCommitters? _committers;
    private Meter? _meter;

    private ReplicaStateHarness()
    {
    }

    /// <summary>Gets the appliers of the served groups.</summary>
    internal required ReplicaGroupAppliers Appliers { get; init; }

    /// <summary>Gets the member state of both metadata slots as the journal barrier saw them, one entry per barrier.</summary>
    internal required List<(GroupMemberState First, GroupMemberState Second)> BarrierStates { get; init; }

    /// <summary>Gets the cache of the node.</summary>
    internal required PhysicalCache<object?> Cache { get; init; }

    /// <summary>Gets the clock of the receiver and of the cache.</summary>
    internal required FakeTimeProvider Clock { get; init; }

    /// <summary>Gets the log the appliers write to, which records a reseed of an applied index as a gap.</summary>
    internal required EventRecordingLogger Events { get; init; }

    /// <summary>Gets the receiver under test.</summary>
    internal required ReplicaTransferReceiver Receiver { get; init; }

    /// <summary>Gets the registry that holds the group log.</summary>
    internal required ReplicaGroupRegistry Registry { get; init; }

    /// <summary>Gets the real sink the receiver writes through.</summary>
    internal required ReplicaStateSink Sink { get; init; }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        Receiver.Dispose();
        await Registry.DisposeAsync();
        if (_committers != null)
            await _committers.DisposeAsync();

        _meter?.Dispose();
    }

    /// <summary>Builds the baseline of the transfer of the standard snapshot.</summary>
    /// <param name="image">The published image of the snapshot.</param>
    /// <returns>The baseline.</returns>
    internal static ReplicaTransferBaseline BaselineOf(byte[] image) => new(
        TransferFixtures.SnapshotIndex,
        TransferFixtures.SnapshotTerm,
        image.Length,
        TransferFixtures.ChecksumOf(image),
        TransferFixtures.Fingerprint,
        1UL);

    /// <summary>Builds the entry a leader sends for a key.</summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value.</param>
    /// <param name="expiresUtcTicks">The absolute deadline in ticks; zero for none.</param>
    /// <returns>The entry.</returns>
    internal static ReplicaStateEntry Entry(string key, string value, long expiresUtcTicks) =>
        new(CacheName, Encoding.UTF8.GetBytes(key), ReplicaCacheApplier.EncodeEntry(new NodeCacheEntry<object?>(value)), expiresUtcTicks);

    /// <summary>Builds the header of a transfer.</summary>
    /// <param name="transferId">The transfer identifier.</param>
    /// <param name="term">The leader term.</param>
    /// <returns>The header.</returns>
    internal static ReplicaTransferHeader HeaderOf(string transferId, ulong term) => new(GroupId, transferId, LeaderId, term);

    /// <summary>Computes the count and the checksum a leader sends at the end of a transfer of entries.</summary>
    /// <param name="entries">The entries in the order they were sent.</param>
    /// <returns>The count and the CRC32C.</returns>
    internal static ReplicaStateTotals TotalsOf(params ReplicaStateEntry[] entries)
    {
        var checksum = new ReplicaStateChecksum();
        for (var i = 0; i < entries.Length; i++)
            checksum.Add(in entries[i]);

        return checksum.Totals;
    }

    /// <summary>Opens a receiver over a data directory.</summary>
    /// <param name="dir">The node data directory; a complete copy holding two entries is seeded in it when the group has no copy yet.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <param name="options">The receiver options, the defaults when not set.</param>
    /// <returns>The harness, over an empty cache.</returns>
    internal static async Task<ReplicaStateHarness> OpenAsync(TempDirectory dir, CancellationToken cancellationToken, ReplicaTransferOptions? options = null)
    {
        if (!File.Exists(GroupStoragePaths.GetMetadataPath(dir, GroupId)))
            await TransferFixtures.SeedBehindAsync(dir, cancellationToken);

        var clock = new FakeTimeProvider(new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var logOptions = new FollowerLogOptions { TimeProvider = clock, ShutdownTimeProvider = TimeProvider.System };
        var registry = new ReplicaGroupRegistry(dir, [GroupId], 3, TransferFixtures.Fingerprint, 1UL, NullLoggerFactory.Instance, logOptions) { ElectionClock = clock };
        await registry.OpenAsync(cancellationToken);
        var physical = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
        var meter = new Meter("state-harness");
        var local = new ClientCache<object?>(physical, physical);
        var events = new EventRecordingLogger();
        var appliers = new ReplicaGroupAppliers(registry, local, "n1", events, new ReplicationMetrics(meter));
        var committers = new ReplicaGroupCommitters([], "n9", Locator(), clock);
        var barriers = new List<(GroupMemberState First, GroupMemberState Second)>();
        var journal = Journal(dir, barriers);
        var sink = new ReplicaStateSink(appliers, committers, local, new ReplicaGroupEntries(physical, Locator()), journal, journal);
        var receiver = new ReplicaTransferReceiver(
            registry,
            RocksDoubles.CreateReplicaMembers(),
            sink,
            options ?? new ReplicaTransferOptions(),
            clock,
            NullLogger<ReplicaTransferReceiver>.Instance);
        return new ReplicaStateHarness
        {
            Appliers = appliers,
            BarrierStates = barriers,
            Cache = physical,
            Clock = clock,
            Events = events,
            Receiver = receiver,
            Registry = registry,
            Sink = sink,
            _committers = committers,
            _meter = meter,
        };
    }

    /// <summary>Begins a transfer and uploads its snapshot image in two chunks.</summary>
    /// <param name="transferId">The transfer identifier.</param>
    /// <param name="term">The leader term.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The sequence number of the next message.</returns>
    /// <exception cref="InvalidOperationException">A message was refused.</exception>
    internal async Task<int> BeginAndUploadAsync(string transferId, ulong term, CancellationToken cancellationToken)
    {
        var image = TransferFixtures.Image();
        var half = image.Length / 2;
        _begun.Add(HeaderOf(transferId, term));
        _ = Accepted(await Receiver.BeginAsync(HeaderOf(transferId, term), BaselineOf(image), cancellationToken));
        _ = Accepted(await Receiver.SnapshotAsync(HeaderOf(transferId, term), 1, 0, image.AsMemory(0, half), cancellationToken));
        _ = Accepted(await Receiver.SnapshotAsync(HeaderOf(transferId, term), 2, half, image.AsMemory(half), cancellationToken));
        return 3;
    }

    /// <summary>Builds the header of a transfer this harness began, with the term it began at.</summary>
    /// <param name="transferId">The transfer identifier.</param>
    /// <returns>The header.</returns>
    /// <exception cref="InvalidOperationException">The harness began no such transfer.</exception>
    internal ReplicaTransferHeader HeaderFor(string transferId)
    {
        for (var i = _begun.Count - 1; i >= 0; i--)
        {
            if (string.Equals(_begun[i].TransferId, transferId, StringComparison.Ordinal))
                return _begun[i];
        }

        throw new InvalidOperationException($"The harness began no transfer '{transferId}'.");
    }

    /// <summary>Stores keys in the cache as an earlier life of the node left them.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <param name="keys">The keys; one that starts with <c language="text">other</c> belongs to another group.</param>
    /// <returns>A task that completes when the keys are stored.</returns>
    internal async Task SeedAsync(CancellationToken cancellationToken, params string[] keys)
    {
        for (var i = 0; i < keys.Length; i++)
            await Cache.SetAsync(new CacheKey(CacheName, keys[i]), new NodeCacheEntry<object?>("old"), cancellationToken);
    }

    /// <summary>Sends one batch of entries.</summary>
    /// <param name="transferId">The transfer identifier.</param>
    /// <param name="sequence">The sequence number of the message.</param>
    /// <param name="entries">The entries.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The sequence number of the next message.</returns>
    /// <exception cref="InvalidOperationException">The message was refused.</exception>
    internal async Task<int> SendBatchAsync(string transferId, int sequence, ReplicaStateEntry[] entries, CancellationToken cancellationToken)
    {
        _ = Accepted(await Receiver.EntriesAsync(HeaderFor(transferId), sequence, entries, cancellationToken));
        return sequence + 1;
    }

    /// <summary>Sends wipe steps until the receiver reports the group has no key left.</summary>
    /// <param name="transferId">The transfer identifier.</param>
    /// <param name="sequence">The sequence number of the first wipe message.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The sequence number of the next message.</returns>
    /// <exception cref="InvalidOperationException">A message was refused.</exception>
    internal async Task<int> WipeAllAsync(string transferId, int sequence, CancellationToken cancellationToken)
    {
        ReplicaTransferReply reply;
        do
            reply = Accepted(await Receiver.WipeAsync(HeaderFor(transferId), sequence++, cancellationToken));
        while (reply.More);

        return sequence;
    }

    private static ReplicaTransferReply Accepted(in ReplicaTransferReply reply) =>
        reply.Success ? reply : throw new InvalidOperationException($"The receiver refused a transfer message: '{reply.RefusalCode}'.");

    private static IJournalCoordinator Journal(string dir, List<(GroupMemberState First, GroupMemberState Second)> barriers)
    {
        var journal = new IJournalCoordinatorCreateExpectations();
        _ = journal.Setups.WaitForStartupAsync(Arg.Any<CancellationToken>()).Callback(static _ => ValueTask.CompletedTask);
        _ = journal.Setups.AwaitDurabilityCommitAsync(Arg.Any<CancellationToken>()).Callback(token => RecordBarrierAsync(dir, barriers, token));
        return journal.Instance();
    }

    private static INodeLocator Locator()
    {
        var locator = new INodeLocatorCreateExpectations();
        _ = locator.Setups.GetOwner(Arg.Any<string>(), Arg.Any<string>()).Callback(static (_, key) => key.StartsWith("other", StringComparison.Ordinal) ? "other" : GroupId);
        return locator.Instance();
    }

    private static async ValueTask RecordBarrierAsync(string dir, List<(GroupMemberState First, GroupMemberState Second)> barriers, CancellationToken cancellationToken)
    {
        var bytes = await GroupMetadataFile.ReadBytesAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), cancellationToken);
        barriers.Add(TransferFixtures.SlotStates(bytes));
    }
}
