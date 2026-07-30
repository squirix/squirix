using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Invocation;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.TestKit.Mtls;

/// <summary>Direct gRPC probes for inter-node mTLS security scenarios in black-box tests.</summary>
public static class InterNodeGrpcProbe
{
    /// <summary>Attempts an owner-routing cache read with optional external JWT and internal-owner metadata.</summary>
    /// <param name="uri">Primary external HTTPS listener URL.</param>
    /// <param name="bearerToken">Optional external bearer token.</param>
    /// <param name="includeInternalOwnerHeader">Whether to include the internal owner-routing marker.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The resulting gRPC status code.</returns>
    public static async Task<StatusCode> TryGetValueAsync(Uri uri, string? bearerToken, bool includeInternalOwnerHeader, CancellationToken cancellationToken)
    {
        using var channel = GrpcChannel.ForAddress(
            uri,
            new GrpcChannelOptions
            {
                HttpHandler = LoopbackHttp.CreateHandler(),
                MaxReceiveMessageSize = EntryLimits.GrpcMaxReceiveMessageSizeBytes,
                MaxSendMessageSize = EntryLimits.GrpcMaxSendMessageSizeBytes,
            });
        var headers = new Metadata();
        if (!string.IsNullOrWhiteSpace(bearerToken))
            headers.Add("authorization", $"Bearer {bearerToken}");

        if (includeInternalOwnerHeader)
            headers.Add(RemoteInvocationContract.InternalOwnerRpcHeaderName, RemoteInvocationContract.InternalOwnerRpcHeaderValue);

        try
        {
            var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
            var request = new GetValueAsyncRequest { CacheName = "default", Key = "internal-owner-probe" };
            _ = await client.GetValueAsync(request, new CallOptions(headers, cancellationToken: cancellationToken)).ResponseAsync.ConfigureAwait(false);
            return StatusCode.OK;
        }
        catch (RpcException ex)
        {
            return ex.StatusCode;
        }
    }
}
