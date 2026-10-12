using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>A receiver of state transfers over a real group log and a scripted sink.</summary>
internal sealed class ReplicaTransferHarness : IAsyncDisposable
{
    /// <summary>The group the harness serves.</summary>
    internal const string GroupId = TransferFixtures.GroupId;

    /// <summary>The leader that sends the transfer.</summary>
    internal const string LeaderId = "n2";

    private ReplicaTransferHarness()
    {
    }

    /// <summary>Gets the clock of the receiver and of the election state.</summary>
    internal required FakeTimeProvider Clock { get; init; }

    /// <summary>Gets the receiver under test.</summary>
    internal required ReplicaTransferReceiver Receiver { get; init; }

    /// <summary>Gets the registry that holds the group log.</summary>
    internal required ReplicaGroupRegistry Registry { get; init; }

    /// <summary>Gets the scripted sink the receiver writes through.</summary>
    internal required RecordingStateSink Sink { get; init; }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        Receiver.Dispose();
        await Registry.DisposeAsync();
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

    /// <summary>Builds the entry a leader sends for a key; the scripted sink reads only the first byte of the payload.</summary>
    /// <param name="key">The key.</param>
    /// <param name="marker">The first byte of the payload.</param>
    /// <returns>The entry.</returns>
    internal static ReplicaStateEntry Entry(string key, byte marker) => new("cache", Encoding.UTF8.GetBytes(key), new byte[] { marker, 1 }, 0L);

    /// <summary>Builds the header of a transfer.</summary>
    /// <param name="transferId">The transfer identifier.</param>
    /// <param name="term">The leader term.</param>
    /// <returns>The header.</returns>
    internal static ReplicaTransferHeader HeaderOf(string transferId, ulong term) => new(GroupId, transferId, LeaderId, term);

    /// <summary>Opens a receiver over a data directory.</summary>
    /// <param name="dir">The node data directory; a complete copy holding two entries is seeded in it when the group has no copy yet.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <param name="options">The receiver options, the defaults when not set.</param>
    /// <returns>The harness.</returns>
    internal static async Task<ReplicaTransferHarness> OpenAsync(TempDirectory dir, CancellationToken cancellationToken, ReplicaTransferOptions? options = null)
    {
        if (!File.Exists(GroupStoragePaths.GetMetadataPath(dir, GroupId)))
            await TransferFixtures.SeedBehindAsync(dir, cancellationToken);

        var clock = new FakeTimeProvider(new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var logOptions = new FollowerLogOptions { TimeProvider = clock, ShutdownTimeProvider = TimeProvider.System };
        var registry = new ReplicaGroupRegistry(dir, [GroupId], 3, TransferFixtures.Fingerprint, 1UL, NullLoggerFactory.Instance, logOptions) { ElectionClock = clock };
        await registry.OpenAsync(cancellationToken);
        var sink = new RecordingStateSink(dir);
        var receiver = new ReplicaTransferReceiver(
            registry,
            RocksDoubles.CreateReplicaMembers(),
            sink,
            options ?? new ReplicaTransferOptions(),
            clock,
            NullLogger<ReplicaTransferReceiver>.Instance);
        return new ReplicaTransferHarness { Clock = clock, Receiver = receiver, Registry = registry, Sink = sink };
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
        _ = Accepted(await Receiver.BeginAsync(HeaderOf(transferId, term), BaselineOf(image), cancellationToken));
        _ = Accepted(await Receiver.SnapshotAsync(GroupId, transferId, 1, 0, image.AsMemory(0, half), cancellationToken));
        _ = Accepted(await Receiver.SnapshotAsync(GroupId, transferId, 2, half, image.AsMemory(half), cancellationToken));
        return 3;
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
        _ = Accepted(await Receiver.EntriesAsync(GroupId, transferId, sequence, entries, cancellationToken));
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
            reply = Accepted(await Receiver.WipeAsync(GroupId, transferId, sequence++, cancellationToken));
        while (reply.More);

        return sequence;
    }

    private static ReplicaTransferReply Accepted(in ReplicaTransferReply reply) =>
        reply.Success ? reply : throw new InvalidOperationException($"The receiver refused a transfer message: '{reply.RefusalCode}'.");
}
