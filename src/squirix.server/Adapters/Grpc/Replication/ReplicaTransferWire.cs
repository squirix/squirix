using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Squirix.Server.Cluster.Replication;

namespace Squirix.Server.Adapters.Grpc.Replication;

/// <summary>Maps the messages of a state transfer between the port types of the replication layer and the wire.</summary>
/// <remarks>The server side hands payload bytes to the receiver as views of the request; the client side wraps the caller buffers without copying them.</remarks>
internal static class ReplicaTransferWire
{
    /// <summary>Creates the abort request of a transfer.</summary>
    /// <param name="header">The envelope identity.</param>
    /// <param name="transferId">The transfer to abort.</param>
    /// <returns>The wire request.</returns>
    internal static TransferReplicaStateRequest AbortRequest(in ReplicaRpcHeader header, string transferId) => new()
    {
        Header = ReplicaRpcGateway.MapHeader(in header),
        TransferId = transferId,
        Abort = new ReplicaTransferAbort(),
    };

    /// <summary>Hands the step of a request to the receiver.</summary>
    /// <param name="receiver">The receiver of the transfers of this node.</param>
    /// <param name="request">The request whose envelope the caller verified.</param>
    /// <param name="cancellationToken">Cancellation token of the call.</param>
    /// <returns>The reply of the receiver, or a refusal when a value of the step is out of range.</returns>
    /// <exception cref="RpcException">The request carries no step.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The step is not supported.</exception>
    internal static async Task<TransferReplicaStateResponse> ApplyAsync(ReplicaTransferReceiver receiver, TransferReplicaStateRequest request, CancellationToken cancellationToken)
    {
        var reply = request.StepCase switch
        {
            TransferReplicaStateRequest.StepOneofCase.Begin => await BeginAsync(receiver, request, cancellationToken).ConfigureAwait(false),
            TransferReplicaStateRequest.StepOneofCase.Snapshot => await SnapshotAsync(receiver, request, cancellationToken).ConfigureAwait(false),
            TransferReplicaStateRequest.StepOneofCase.Wipe => await WipeAsync(receiver, request, cancellationToken).ConfigureAwait(false),
            TransferReplicaStateRequest.StepOneofCase.Entries => await EntriesAsync(receiver, request, cancellationToken).ConfigureAwait(false),
            TransferReplicaStateRequest.StepOneofCase.End => await EndAsync(receiver, request, cancellationToken).ConfigureAwait(false),
            TransferReplicaStateRequest.StepOneofCase.Abort => await receiver.AbortAsync(SenderOf(request), cancellationToken).ConfigureAwait(false),
            TransferReplicaStateRequest.StepOneofCase.None => throw new RpcException(new Status(StatusCode.InvalidArgument, "A state transfer request with a step is required.")),
            _ => throw new ArgumentOutOfRangeException(nameof(request), request.StepCase, "Unsupported transfer step."),
        };
        return new TransferReplicaStateResponse { Term = reply.Term, Success = reply.Success, RefusalCode = reply.RefusalCode, More = reply.More };
    }

    /// <summary>Creates the begin request of a transfer.</summary>
    /// <param name="header">The envelope identity; it carries the fingerprint and the generation of the baseline.</param>
    /// <param name="transferId">The identifier of the transfer.</param>
    /// <param name="baseline">The point the transfer is taken at.</param>
    /// <returns>The wire request.</returns>
    internal static TransferReplicaStateRequest BeginRequest(in ReplicaRpcHeader header, string transferId, in ReplicaTransferBaseline baseline) => new()
    {
        Header = ReplicaRpcGateway.MapHeader(in header),
        TransferId = transferId,
        Sequence = 0U,
        Begin = new ReplicaTransferBegin
        {
            Index = baseline.Index,
            IndexTerm = baseline.IndexTerm,
            SnapshotBytes = uint.CreateChecked(baseline.SnapshotBytes),
            SnapshotChecksum = baseline.SnapshotChecksum,
            CeilingChecked = baseline.CeilingChecked,
        },
    };

    /// <summary>Creates the request of a message after the begin.</summary>
    /// <param name="header">The envelope identity.</param>
    /// <param name="message">The message.</param>
    /// <returns>The wire request.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The sequence number or the offset is negative, or the step is not supported.</exception>
    internal static TransferReplicaStateRequest MessageRequest(in ReplicaRpcHeader header, in ReplicaTransferMessage message)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(message.Sequence);
        ArgumentOutOfRangeException.ThrowIfNegative(message.Offset);
        var request = new TransferReplicaStateRequest
        {
            Header = ReplicaRpcGateway.MapHeader(in header),
            TransferId = message.TransferId,
            Sequence = uint.CreateChecked(message.Sequence),
        };
        switch (message.Step)
        {
            case ReplicaTransferStep.Snapshot:
                request.Snapshot = new ReplicaTransferSnapshot { Offset = ulong.CreateChecked(message.Offset), Chunk = UnsafeByteOperations.UnsafeWrap(message.Chunk) };
                break;
            case ReplicaTransferStep.Wipe:
                request.Wipe = new ReplicaTransferWipe();
                break;
            case ReplicaTransferStep.Entries:
                request.Entries = MapEntries(message.Entries.Span);
                break;
            case ReplicaTransferStep.End:
                request.End = new ReplicaTransferEnd { EntryCount = message.EntryCount, EntriesCrc32C = message.EntriesCrc };
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(message), message.Step, "Unsupported transfer step.");
        }

        return request;
    }

    /// <summary>Maps the response of a receiver.</summary>
    /// <param name="response">The wire response.</param>
    /// <returns>The reply.</returns>
    internal static ReplicaTransferReply Reply(TransferReplicaStateResponse response) => new(response.Term, response.Success, response.RefusalCode, response.More);

    private static async Task<ReplicaTransferReply> BeginAsync(ReplicaTransferReceiver receiver, TransferReplicaStateRequest request, CancellationToken cancellationToken)
    {
        var begin = request.Begin;
        if (begin.SnapshotBytes > int.MaxValue)
            return OutOfRange(receiver, request, "Begin", "snapshot-length-out-of-range");

        var envelope = request.Header;
        var baseline = new ReplicaTransferBaseline(
            begin.Index,
            begin.IndexTerm,
            int.CreateChecked(begin.SnapshotBytes),
            begin.SnapshotChecksum,
            envelope.TopologyFingerprint.Memory,
            envelope.ConfigurationGeneration,
            begin.CeilingChecked);
        return await receiver.BeginAsync(SenderOf(request), baseline, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<ReplicaTransferReply> EndAsync(ReplicaTransferReceiver receiver, TransferReplicaStateRequest request, CancellationToken cancellationToken)
    {
        var end = request.End;
        return request.Sequence > int.MaxValue
            ? OutOfRange(receiver, request, "End", "sequence-number-out-of-range")
            : await receiver.EndAsync(SenderOf(request), int.CreateChecked(request.Sequence), end.EntryCount, end.EntriesCrc32C, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<ReplicaTransferReply> EntriesAsync(ReplicaTransferReceiver receiver, TransferReplicaStateRequest request, CancellationToken cancellationToken)
    {
        if (request.Sequence > int.MaxValue)
            return OutOfRange(receiver, request, "Entries", "sequence-number-out-of-range");

        var wire = request.Entries.Entries;
        var entries = new ReplicaStateEntry[wire.Count];
        for (var i = 0; i < entries.Length; i++)
        {
            var entry = wire[i];
            entries[i] = new ReplicaStateEntry(entry.CacheName, entry.Key.Memory, entry.Payload.Memory, entry.ExpiresUtcTicks);
        }

        return await receiver.EntriesAsync(SenderOf(request), int.CreateChecked(request.Sequence), entries, cancellationToken).ConfigureAwait(false);
    }

    private static ReplicaTransferEntries MapEntries(ReadOnlySpan<ReplicaStateEntry> entries)
    {
        var batch = new ReplicaTransferEntries();
        for (var i = 0; i < entries.Length; i++)
        {
            batch.Entries.Add(
                new ReplicaTransferEntry
                {
                    CacheName = entries[i].CacheName,
                    Key = UnsafeByteOperations.UnsafeWrap(entries[i].Key),
                    Payload = UnsafeByteOperations.UnsafeWrap(entries[i].Payload),
                    ExpiresUtcTicks = entries[i].ExpiresUtcTicks,
                });
        }

        return batch;
    }

    /// <summary>Names the transfer of a request with the sender the adapter verified against the peer certificate and the term of the envelope.</summary>
    /// <param name="request">The request whose envelope the caller verified.</param>
    /// <returns>The transfer identity every step and the abort are bound to.</returns>
    private static ReplicaTransferHeader SenderOf(TransferReplicaStateRequest request) => new(
        request.Header.GroupId,
        request.TransferId,
        request.Header.LeaderNodeId,
        request.Header.Term);

    private static ReplicaTransferReply OutOfRange(ReplicaTransferReceiver receiver, TransferReplicaStateRequest request, string step, string reason) =>
        receiver.Refuse(request.Header.GroupId, 0UL, RefusalCodes.ChecksumMismatch, SenderOf(request), reason, step);

    private static async Task<ReplicaTransferReply> WipeAsync(ReplicaTransferReceiver receiver, TransferReplicaStateRequest request, CancellationToken cancellationToken) =>
        request.Sequence > int.MaxValue ? OutOfRange(receiver, request, "Wipe", "sequence-number-out-of-range") : await receiver.WipeAsync(SenderOf(request), int.CreateChecked(request.Sequence), cancellationToken).ConfigureAwait(false);

    private static async Task<ReplicaTransferReply> SnapshotAsync(ReplicaTransferReceiver receiver, TransferReplicaStateRequest r, CancellationToken cancellationToken)
    {
        var s = r.Snapshot;
        return true switch
        {
            _ when r.Sequence > int.MaxValue => OutOfRange(receiver, r, "Snapshot", "sequence-number-out-of-range"),
            _ when s.Offset > long.MaxValue => OutOfRange(receiver, r, "Snapshot", "snapshot-offset-out-of-range"),
            _ => await receiver.SnapshotAsync(SenderOf(r), int.CreateChecked(r.Sequence), long.CreateChecked(s.Offset), s.Chunk.Memory, cancellationToken).ConfigureAwait(false),
        };
    }
}
