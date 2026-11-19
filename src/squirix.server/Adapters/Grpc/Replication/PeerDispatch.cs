using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Google.Protobuf;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Node.Observability;
using Squirix.Transport.Binary;

namespace Squirix.Server.Adapters.Grpc.Replication;

internal static class PeerDispatch
{
    internal static Task HandleAsync(HttpContext http) => PeerEndpoint.HandleAsync(http, DispatchAsync);

    private static async Task<BinaryResponse> DispatchAsync(HttpContext http, ReadOnlyMemory<byte> body, string peer)
    {
        var path = http.Request.Path.Value!;
        var correlation = http.RequestServices.GetRequiredService<ServerCallCorrelation>();
        using var activity = ServerCallCorrelation.StartActivity(path, http.Request.Headers[Correlation.TraceParentHeader], http.Request.Headers[Correlation.TraceStateHeader]);
        using var logScope = correlation.BeginLogScope(path);
        var service = http.RequestServices.GetRequiredService<SquirixReplicationServiceAdapter>();
        IMessage? response = http.Request.RouteValues["operation"] switch
        {
            "AppendReplicaEntries" => await service.AppendReplicaEntriesAsync(AppendReplicaEntriesRequest.Parser.ParseFrom(body.Span), http).ConfigureAwait(false),
            "GetReplicaStatus" => await service.GetReplicaStatusAsync(GetReplicaStatusRequest.Parser.ParseFrom(body.Span), http).ConfigureAwait(false),
            "OfferReplicaBootstrap" => await service.OfferReplicaBootstrapAsync(OfferReplicaBootstrapRequest.Parser.ParseFrom(body.Span), http).ConfigureAwait(false),
            "PreVote" => await service.PreVoteAsync(ReplicaVoteRequest.Parser.ParseFrom(body.Span), http).ConfigureAwait(false),
            "RequestVote" => await service.RequestVoteAsync(ReplicaVoteRequest.Parser.ParseFrom(body.Span), http).ConfigureAwait(false),
            "TransferReplicaState" => await service.TransferReplicaStateAsync(ParseTransfer(body), http).ConfigureAwait(false),
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
