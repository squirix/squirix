using System;
using System.Threading.Tasks;
using Google.Protobuf;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Transport.Binary;

namespace Squirix.Server.Adapters.Grpc.Replication;

internal static class PeerDispatch
{
    internal static Task HandleAsync(HttpContext http) => PeerEndpoint.HandleAsync(http, DispatchAsync);

    private static async Task<BinaryResponse> DispatchAsync(HttpContext http, ReadOnlyMemory<byte> body, string peer)
    {
        var service = http.RequestServices.GetRequiredService<SquirixReplicationServiceAdapter>();
        IMessage? response = http.Request.RouteValues["operation"] switch
        {
            "AppendReplicaEntries" => await service.AppendReplicaEntriesAsync(AppendReplicaEntriesRequest.Parser.ParseFrom(body.Span), http).ConfigureAwait(false),
            "GetReplicaStatus" => await service.GetReplicaStatusAsync(GetReplicaStatusRequest.Parser.ParseFrom(body.Span), http).ConfigureAwait(false),
            "OfferReplicaBootstrap" => await service.OfferReplicaBootstrapAsync(OfferReplicaBootstrapRequest.Parser.ParseFrom(body.Span), http).ConfigureAwait(false),
            "PreVote" => await service.PreVoteAsync(ReplicaVoteRequest.Parser.ParseFrom(body.Span), http).ConfigureAwait(false),
            "RequestVote" => await service.RequestVoteAsync(ReplicaVoteRequest.Parser.ParseFrom(body.Span), http).ConfigureAwait(false),
            _ => null,
        };
        return response == null ? new BinaryResponse(FrameStatus.Unimplemented) : new BinaryResponse(FrameStatus.Ok, response);
    }
}
