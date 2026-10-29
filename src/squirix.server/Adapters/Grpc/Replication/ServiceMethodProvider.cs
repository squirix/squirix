using System.Collections.Generic;
using Google.Protobuf;
using Grpc.AspNetCore.Server.Model;
using Grpc.Core;
using Microsoft.AspNetCore.Routing;

namespace Squirix.Server.Adapters.Grpc.Replication;

/// <summary>Binds the closed replication contract with pooled deserialization for state transfers.</summary>
internal sealed class ServiceMethodProvider : IServiceMethodProvider<SquirixReplicationServiceAdapter>
{
    private static readonly string ServiceName = SquirixReplicationService.Descriptor.FullName;
    private readonly ReplicaTransferMarshaller _transfers;

    public ServiceMethodProvider(ReplicaTransferMarshaller transfers)
    {
        _transfers = transfers;
    }

    /// <inheritdoc />
    public void OnServiceMethodDiscovery(ServiceMethodProviderContext<SquirixReplicationServiceAdapter> context)
    {
        Add(
            context,
            nameof(SquirixReplicationService.SquirixReplicationServiceBase.AppendReplicaEntries),
            AppendReplicaEntriesRequest.Parser,
            AppendReplicaEntriesResponse.Parser,
            static (service, request, call) => service.AppendReplicaEntriesAsync(request, call));
        Add(
            context,
            nameof(SquirixReplicationService.SquirixReplicationServiceBase.GetReplicaStatus),
            GetReplicaStatusRequest.Parser,
            GetReplicaStatusResponse.Parser,
            static (service, request, call) => service.GetReplicaStatusAsync(request, call));
        Add(
            context,
            nameof(SquirixReplicationService.SquirixReplicationServiceBase.OfferReplicaBootstrap),
            OfferReplicaBootstrapRequest.Parser,
            OfferReplicaBootstrapResponse.Parser,
            static (service, request, call) => service.OfferReplicaBootstrapAsync(request, call));
        Add(
            context,
            nameof(SquirixReplicationService.SquirixReplicationServiceBase.PreVote),
            ReplicaVoteRequest.Parser,
            ReplicaVoteResponse.Parser,
            static (service, request, call) => service.PreVoteAsync(request, call));
        Add(
            context,
            nameof(SquirixReplicationService.SquirixReplicationServiceBase.RequestVote),
            ReplicaVoteRequest.Parser,
            ReplicaVoteResponse.Parser,
            static (service, request, call) => service.RequestVoteAsync(request, call));
        var transfer = new Method<TransferReplicaStateRequest, TransferReplicaStateResponse>(
            MethodType.Unary,
            ServiceName,
            nameof(SquirixReplicationService.SquirixReplicationServiceBase.TransferReplicaState),
            new Marshaller<TransferReplicaStateRequest>(Serialize, _transfers.Deserialize),
            Marshaller(TransferReplicaStateResponse.Parser));
        context.AddUnaryMethod(transfer, Metadata(), static (service, request, call) => service.TransferReplicaStateAsync(request, call));
    }

    private static void Add<TRequest, TResponse>(
        ServiceMethodProviderContext<SquirixReplicationServiceAdapter> context,
        string name,
        MessageParser<TRequest> request,
        MessageParser<TResponse> response,
        UnaryServerMethod<SquirixReplicationServiceAdapter, TRequest, TResponse> handler)
        where TRequest : class, IMessage<TRequest>
        where TResponse : class, IMessage<TResponse> =>
        context.AddUnaryMethod(new Method<TRequest, TResponse>(MethodType.Unary, ServiceName, name, Marshaller(request), Marshaller(response)), Metadata(), handler);

    private static Marshaller<T> Marshaller<T>(MessageParser<T> parser)
        where T : class, IMessage<T> => new(Serialize, context => parser.ParseFrom(context.PayloadAsReadOnlySequence()));

    private static List<object> Metadata() => [new HttpMethodMetadata(["POST"], true)];

    private static void Serialize<T>(T message, SerializationContext context)
        where T : IMessage<T>
    {
        context.SetPayloadLength(message.CalculateSize());
        message.WriteTo(context.GetBufferWriter());
        context.Complete();
    }
}
