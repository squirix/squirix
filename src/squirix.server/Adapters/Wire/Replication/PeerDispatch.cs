using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Node.Observability;
using Squirix.Transport.Binary;

namespace Squirix.Server.Adapters.Wire.Replication;

internal static class PeerDispatch
{
    internal static async Task<BinaryResponse> DispatchAsync(string operation, ReadOnlyMemory<byte> body, PeerCallContext context, SquirixReplicationServiceAdapter service)
    {
        IMessage? response = operation switch
        {
            "AppendReplicaEntries" => await service.AppendReplicaEntriesAsync(AppendReplicaEntriesRequest.Parser.ParseFrom(body.Span), context).ConfigureAwait(false),
            "GetReplicaStatus" => await service.GetReplicaStatusAsync(GetReplicaStatusRequest.Parser.ParseFrom(body.Span), context).ConfigureAwait(false),
            "OfferReplicaBootstrap" => await service.OfferReplicaBootstrapAsync(OfferReplicaBootstrapRequest.Parser.ParseFrom(body.Span), context).ConfigureAwait(false),
            "PreVote" => await service.PreVoteAsync(ReplicaVoteRequest.Parser.ParseFrom(body.Span), context).ConfigureAwait(false),
            "RequestVote" => await service.RequestVoteAsync(ReplicaVoteRequest.Parser.ParseFrom(body.Span), context).ConfigureAwait(false),
            "TransferReplicaState" => await service.TransferReplicaStateAsync(ParseTransfer(body), context).ConfigureAwait(false),
            _ => null,
        };
        return response == null ? new BinaryResponse(FrameStatus.Unimplemented) : new BinaryResponse(FrameStatus.Ok, response);
    }

    private static TransferReplicaStateRequest ParseTransfer(ReadOnlyMemory<byte> body)
    {
        try
        {
            return MemoryMarshal.TryGetArray(body, out var segment) && segment.Offset == 0
                ? ReplicaTransferParser.Parse(segment.Array!, segment.Count)
                : throw new InvalidOperationException("State transfer requires the rented request buffer.");
        }
        catch (InvalidDataException error)
        {
            throw new ServerCallException(FrameStatus.InvalidArgument, "Malformed state transfer.", error);
        }
    }
}
