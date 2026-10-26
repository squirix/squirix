using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>Streams the state of one replica group to one follower: the snapshot image, the wipe, the entries of the group and the end, one request at a time.</summary>
/// <remarks>
/// <para>
/// A transfer is a begin, the chunks of the published snapshot image of at most one MiB, wipe steps repeated while the receiver reports more, the
/// entries of the group in batches under the message budget, and an end that carries the count and the CRC32C of the entries. Every request is
/// bounded by the step timeout and by the cancellation of the pass and of the sender behind the follower, so a retirement ends it. The walk of the
/// entries runs while writes continue: every effect is absolute per key and the follower replays the log above the snapshot index afterwards.
/// </para>
/// <para>
/// A follower whose journal is at its high-water mark answers journal-busy and keeps the transfer: the stream waits, with a growing delay, and sends
/// the identical message again. Every accepted reply restarts the stall clock; a follower that stays busy for the stall budget gets an abort and the
/// stream ends as journal-full. Any other refusal ends the stream without an abort; a transport fault or a timeout sends one abort, best effort. Not
/// thread-safe: one transfer runs one request at a time.
/// </para>
/// </remarks>
internal sealed class ReplicaTransferStream
{
    /// <summary>The most bytes of the snapshot image one chunk carries.</summary>
    private const int ChunkBytes = 1024 * 1024;

    /// <summary>The longest delay between two sends of a message the follower answered as busy.</summary>
    private static readonly TimeSpan BusyBackoffMax = TimeSpan.FromSeconds(2);

    /// <summary>The first delay before a message the follower answered as busy is sent again.</summary>
    private static readonly TimeSpan BusyBackoffStart = TimeSpan.FromMilliseconds(250);

    private readonly ReplicaStateChecksum _checksum = new();
    private readonly TimeProvider _clock;
    private readonly IReplicaTransferGateway _gateway;
    private readonly ReplicaRpcHeader _header;
    private readonly string _nodeId;
    private readonly ReplicaTransferOptions _options;
    private int _sequence;

    /// <summary>Initializes a new instance of the <see cref="ReplicaTransferStream" /> class.</summary>
    /// <param name="gateway">The transfer RPCs.</param>
    /// <param name="options">The message limits and the step timeout.</param>
    /// <param name="header">The envelope of the leader: the group, the topology and the term of the transfer.</param>
    /// <param name="nodeId">The follower the state goes to.</param>
    /// <param name="clock">The time source of the step timeouts.</param>
    internal ReplicaTransferStream(IReplicaTransferGateway gateway, ReplicaTransferOptions options, in ReplicaRpcHeader header, string nodeId, TimeProvider clock)
    {
        ArgumentNullException.ThrowIfNull(gateway);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        ArgumentNullException.ThrowIfNull(clock);
        _gateway = gateway;
        _options = options;
        _header = header;
        _nodeId = nodeId;
        _clock = clock;
    }

    /// <summary>Gets the bytes the stream sent: the snapshot image and the entries as the message budget counts them.</summary>
    internal long Bytes { get; private set; }

    /// <summary>Initializes a value indicating whether the leader checked the term ceiling, which lets the transfer repair a copy that lost its disk.</summary>
    internal bool CeilingChecked { private get; init; }

    /// <summary>Initializes the token that ends the stream when the sender behind the follower starts draining or closes.</summary>
    internal CancellationToken Draining { private get; init; }

    /// <summary>Gets the number of entries of the group the stream sent.</summary>
    internal ulong Entries => _checksum.Totals.EntryCount;

    /// <summary>Initializes the callback that runs once when the follower starts a stall by answering busy; it must not throw or wait.</summary>
    internal Action? Paused { private get; init; }

    /// <summary>Initializes the callback that sees every reply of the follower, accepted or not; it must not throw or wait.</summary>
    internal Action<ReplicaTransferReply>? Replies { private get; init; }

    /// <summary>Runs the transfer.</summary>
    /// <param name="snapshot">The group snapshot at the captured applied index.</param>
    /// <param name="walk">The walk over the stored entries of the node.</param>
    /// <param name="cancellationToken">Cancellation token of the pass; its cancellation ends the stream by throwing.</param>
    /// <returns>How the stream ended, and the refusal code of the follower when it refused.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken" /> was canceled.</exception>
    internal async Task<Result> RunAsync(GroupSnapshot snapshot, ReplicaGroupEntries walk, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(walk);
        int length;
        try
        {
            length = GroupSnapshotStore.ComputePublishedLength(in snapshot, _options.MaxSnapshotBytes);
        }
        catch (GroupSnapshotTooLargeException)
        {
            return new Result(ReplicaTransferOutcome.SnapshotTooLarge, string.Empty);
        }

        // The image stays unchanged until the last request returned, which the pass awaits before it gives the buffer back.
        var rented = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            var image = rented.AsMemory(0, length);
            GroupSnapshotStore.EncodePublished(image.Span, in snapshot);
            var crc = BinaryPrimitives.ReadUInt32LittleEndian(image.Span[^sizeof(uint)..]);
            var baseline = new ReplicaTransferBaseline(
                snapshot.LastIncludedIndex,
                snapshot.LastIncludedTerm,
                length,
                crc,
                _header.TopologyFingerprint,
                _header.ConfigurationGeneration,
                CeilingChecked);
            return await RunGuardedAsync(baseline, image, walk, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static bool IsTransportFault(Exception exception) => exception is RpcException or IOException or HttpRequestException or TimeoutException or InvalidOperationException;

    private Result? Ended(in ReplicaTransferReply reply) => reply.Success ? null : Refused(in reply);

    private Result Refused(in ReplicaTransferReply reply) => reply.Term > _header.Term
        ? new Result(ReplicaTransferOutcome.Superseded, string.Empty)
        : new Result(ReplicaTransferOutcome.FollowerRefused, reply.RefusalCode);

    private ReplicaTransferMessage Next(ReplicaTransferStep step, string transferId) =>
        new() { Step = step, GroupId = _header.GroupId, TransferId = transferId, Sequence = ++_sequence };

    /// <summary>Sends the best-effort abort of a transfer that failed in transport or timed out; the receiver drops a silent transfer on its own.</summary>
    /// <param name="transferId">The transfer to abort.</param>
    /// <returns>A task that completes when the abort was answered, failed, or timed out.</returns>
    private async Task AbortAsync(string transferId)
    {
        using var timeout = new CancellationTokenSource(_options.StepTimeout, _clock);
        try
        {
            _ = await _gateway.AbortAsync(_nodeId, _header, transferId, timeout.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested)
        {
            // The follower does not answer: its idle timeout releases the group.
        }
        catch (Exception exception) when (IsTransportFault(exception))
        {
            // The follower is unreachable: its idle timeout releases the group.
        }
    }

    /// <summary>Sends one request bounded by the step timeout and hands its reply to the callback.</summary>
    /// <param name="call">The request.</param>
    /// <param name="token">The token of the stream.</param>
    /// <returns>The reply.</returns>
    /// <exception cref="TimeoutException">The follower did not answer within the step timeout.</exception>
    private async Task<ReplicaTransferReply> CallAsync(Func<CancellationToken, Task<ReplicaTransferReply>> call, CancellationToken token)
    {
        using var timeout = new CancellationTokenSource(_options.StepTimeout, _clock);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(token, timeout.Token);
        ReplicaTransferReply reply;
        try
        {
            reply = await call(linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException canceled) when (timeout.IsCancellationRequested && !token.IsCancellationRequested)
        {
            throw new TimeoutException($"The follower '{_nodeId}' did not answer a state transfer message within {_options.StepTimeout}.", canceled);
        }

        Replies?.Invoke(reply);
        return reply;
    }

    /// <summary>Sends one request and, while the follower answers journal-busy, sends the identical request again after a growing delay.</summary>
    /// <param name="transferId">The transfer, which is aborted when the stall budget runs out.</param>
    /// <param name="call">The request.</param>
    /// <param name="token">The token of the stream; a drain or a cancellation ends the wait by throwing.</param>
    /// <returns>The first reply that is not journal-busy; journal-full when the follower stayed busy for the stall budget.</returns>
    private async Task<ReplicaTransferReply> CallPatientlyAsync(string transferId, Func<CancellationToken, Task<ReplicaTransferReply>> call, CancellationToken token)
    {
        var delay = BusyBackoffStart;
        long? stalledSince = null;
        while (true)
        {
            var reply = await CallAsync(call, token).ConfigureAwait(false);
            if (reply.Success || !string.Equals(reply.RefusalCode, RefusalCodes.JournalBusy, StringComparison.Ordinal) || reply.Term > _header.Term)
                return reply;

            if (stalledSince is not { } since)
            {
                stalledSince = _clock.GetTimestamp();
                Paused?.Invoke();
            }
            else if (_clock.GetElapsedTime(since) >= _options.JournalStallBudget)
            {
                await AbortAsync(transferId).ConfigureAwait(false);
                return ReplicaTransferReply.Refused(reply.Term, RefusalCodes.JournalFull);
            }

            await Task.Delay(delay, _clock, token).ConfigureAwait(false);
            delay = TimeSpan.FromTicks(Math.Min(BusyBackoffMax.Ticks, delay.Ticks * 2));
        }
    }

    private async Task<Result?> EntriesAsync(string transferId, ReplicaGroupEntries walk, CancellationToken token)
    {
        using var batch = new ReplicaTransferBatch();
        await foreach (var item in walk.EnumerateAsync(_header.GroupId, token).ConfigureAwait(false))
        {
            var payload = ReplicaCacheApplier.EncodeEntry(item.Entry);
            var size = ReplicaTransferBatch.SizeOf(item.CacheName, item.Key, payload.Length);
            if (!batch.Fits(size) && await FlushAsync(transferId, batch, token).ConfigureAwait(false) is { } ended)
                return ended;

            var entry = batch.Add(item.CacheName, item.Key, payload, item.Entry.ExpiresUtc?.Ticks ?? 0L, size);
            _checksum.Add(in entry);
        }

        return batch.Count == 0 ? null : await FlushAsync(transferId, batch, token).ConfigureAwait(false);
    }

    private async Task<Result?> FinishAsync(string transferId, CancellationToken token)
    {
        var totals = _checksum.Totals;
        var end = Next(ReplicaTransferStep.End, transferId) with { EntryCount = totals.EntryCount, EntriesCrc = totals.Crc };
        var reply = await SendAsync(end, token).ConfigureAwait(false);
        return Ended(in reply);
    }

    /// <summary>Sends the entries of the batch and empties it once the call returned, so the memory the gateway holds stays unchanged until then.</summary>
    /// <param name="transferId">The transfer.</param>
    /// <param name="batch">The batch.</param>
    /// <param name="token">The token of the stream.</param>
    /// <returns>How the stream ends when the follower refused the batch; otherwise <see langword="null" />.</returns>
    private async Task<Result?> FlushAsync(string transferId, ReplicaTransferBatch batch, CancellationToken token)
    {
        var message = Next(ReplicaTransferStep.Entries, transferId) with { Entries = batch.Entries };
        var reply = await SendAsync(message, token).ConfigureAwait(false);
        Bytes += batch.Bytes;
        batch.Clear();
        return Ended(in reply);
    }

    /// <summary>Runs the steps of the transfer, sending an abort when a message fails in transport or times out.</summary>
    /// <param name="baseline">The point the transfer is taken at and the snapshot image that follows.</param>
    /// <param name="image">The published snapshot image.</param>
    /// <param name="walk">The walk over the stored entries of the node.</param>
    /// <param name="cancellationToken">Cancellation token of the pass.</param>
    /// <returns>How the stream ended.</returns>
    private async Task<Result> RunGuardedAsync(ReplicaTransferBaseline baseline, ReadOnlyMemory<byte> image, ReplicaGroupEntries walk, CancellationToken cancellationToken)
    {
        var transferId = Guid.NewGuid().ToString("N");
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, Draining);
        try
        {
            return await StreamAsync(transferId, baseline, image, walk, linked.Token).ConfigureAwait(false);
        }
        catch (Exception exception) when (IsTransportFault(exception))
        {
            // A call canceled by the pass surfaces as a transport fault too; the cancellation wins and no abort is sent.
            cancellationToken.ThrowIfCancellationRequested();

            // A drain cancels the call in flight, which surfaces as a transport fault: the follower was not at fault.
            await AbortAsync(transferId).ConfigureAwait(false);
            return new Result(Draining.IsCancellationRequested ? ReplicaTransferOutcome.Aborted : ReplicaTransferOutcome.Unreachable, string.Empty);
        }
        catch (OperationCanceledException) when (Draining.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            await AbortAsync(transferId).ConfigureAwait(false);
            return new Result(ReplicaTransferOutcome.Aborted, string.Empty);
        }
    }

    private Task<ReplicaTransferReply> SendAsync(ReplicaTransferMessage message, CancellationToken token) =>
        CallPatientlyAsync(message.TransferId, call => _gateway.SendAsync(_nodeId, _header, message, call), token);

    private async Task<Result> StreamAsync(string transferId, ReplicaTransferBaseline baseline, ReadOnlyMemory<byte> image, ReplicaGroupEntries walk, CancellationToken token)
    {
        var begin = await CallPatientlyAsync(transferId, call => _gateway.BeginAsync(_nodeId, _header, transferId, baseline, call), token).ConfigureAwait(false);
        return Ended(in begin) ??
               await UploadAsync(transferId, image, token).ConfigureAwait(false) ??
               await WipeAsync(transferId, token).ConfigureAwait(false) ??
               await EntriesAsync(transferId, walk, token).ConfigureAwait(false) ??
               await FinishAsync(transferId, token).ConfigureAwait(false) ??
               new Result(ReplicaTransferOutcome.Transferred, string.Empty);
    }

    private async Task<Result?> UploadAsync(string transferId, ReadOnlyMemory<byte> image, CancellationToken token)
    {
        for (var offset = 0; offset < image.Length; offset += ChunkBytes)
        {
            var chunk = Next(ReplicaTransferStep.Snapshot, transferId) with { Offset = offset, Chunk = image.Slice(offset, Math.Min(ChunkBytes, image.Length - offset)) };
            var reply = await SendAsync(chunk, token).ConfigureAwait(false);
            if (Ended(in reply) is { } ended)
                return ended;
        }

        Bytes += image.Length;
        return null;
    }

    private async Task<Result?> WipeAsync(string transferId, CancellationToken token)
    {
        ReplicaTransferReply reply;
        do
        {
            reply = await SendAsync(Next(ReplicaTransferStep.Wipe, transferId), token).ConfigureAwait(false);
            if (Ended(in reply) is { } ended)
                return ended;
        }
        while (reply.More);

        return null;
    }

    /// <summary>How a stream ended.</summary>
    /// <param name="Outcome">The outcome.</param>
    /// <param name="Reason">The refusal code of the follower when it refused; empty otherwise.</param>
    [Immutable]
    internal readonly record struct Result(ReplicaTransferOutcome Outcome, string Reason);
}
