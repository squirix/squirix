using System;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Wire.Replication;
using Squirix.Server.Cluster;
using Squirix.Transport.Binary;

namespace Squirix.Server.Adapters.Endpoint;

internal static class PeerDispatch
{
    internal static async Task<BinaryResponse> DispatchAsync(PeerOperation operation, ReadOnlyMemory<byte> body, PeerCallContext context, SquirixReplicationServiceAdapter service)
    {
        WireMessage? response = operation switch
        {
            PeerOperation.AppendReplicaEntries => await service.AppendReplicaEntriesAsync(AppendReplicaEntriesRequest.Parser.ParseFrom(body.Span), context).ConfigureAwait(false),
            PeerOperation.GetReplicaStatus => await service.GetReplicaStatusAsync(GetReplicaStatusRequest.Parser.ParseFrom(body.Span), context).ConfigureAwait(false),
            PeerOperation.OfferReplicaBootstrap => await service.OfferReplicaBootstrapAsync(OfferReplicaBootstrapRequest.Parser.ParseFrom(body.Span), context).ConfigureAwait(false),
            PeerOperation.PreVote => await service.PreVoteAsync(ReplicaVoteRequest.Parser.ParseFrom(body.Span), context).ConfigureAwait(false),
            PeerOperation.RequestVote => await service.RequestVoteAsync(ReplicaVoteRequest.Parser.ParseFrom(body.Span), context).ConfigureAwait(false),
            PeerOperation.TransferReplicaState => await service.TransferReplicaStateAsync(TransferReplicaStateRequest.Parser.ParseBorrowed(body), context).ConfigureAwait(false),
            _ => null,
        };
        return response == null ? new BinaryResponse(FrameStatus.Unimplemented) : new BinaryResponse(FrameStatus.Ok, response);
    }
}
