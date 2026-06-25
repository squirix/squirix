using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client;
using Squirix.Server.Limits;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.TestKit.Mtls;

/// <summary>Direct gRPC probes for inter-node mTLS security scenarios in black-box tests.</summary>
public static class MtlsInterNodeGrpcProbe
{
    /// <summary>Attempts an owner-routing cache read with optional external JWT and internal-owner metadata.</summary>
    /// <param name="primaryUrl">Primary external HTTPS listener URL.</param>
    /// <param name="bearerToken">Optional external bearer token.</param>
    /// <param name="includeInternalOwnerHeader">Whether to include the internal owner-routing marker.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The resulting gRPC status code.</returns>
    public static Task<StatusCode> TryGetValueAsync(string primaryUrl, string? bearerToken, bool includeInternalOwnerHeader, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(primaryUrl);
        return TryGetValueAsync(new Uri(primaryUrl, UriKind.Absolute), bearerToken, includeInternalOwnerHeader, cancellationToken);
    }

    /// <summary>Attempts an owner-routing cache read with optional external JWT and internal-owner metadata.</summary>
    /// <param name="primaryUrl">Primary external HTTPS listener URL.</param>
    /// <param name="bearerToken">Optional external bearer token.</param>
    /// <param name="includeInternalOwnerHeader">Whether to include the internal owner-routing marker.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The resulting gRPC status code.</returns>
    private static async Task<StatusCode> TryGetValueAsync(Uri primaryUrl, string? bearerToken, bool includeInternalOwnerHeader, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(primaryUrl);

        using var channel = GrpcChannel.ForAddress(
            primaryUrl,
            new GrpcChannelOptions
            {
                HttpHandler = LoopbackHttp.CreateHandler(),
                MaxReceiveMessageSize = SquirixEntryLimits.GrpcMaxReceiveMessageSizeBytes,
                MaxSendMessageSize = SquirixEntryLimits.GrpcMaxSendMessageSizeBytes,
            });
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var headers = new Metadata();
        if (!string.IsNullOrWhiteSpace(bearerToken))
            headers.Add("authorization", $"Bearer {bearerToken}");

        if (includeInternalOwnerHeader)
        {
            headers.Add(RemoteInvocationContract.InternalOwnerRpcHeaderName, RemoteInvocationContract.InternalOwnerRpcHeaderValue);
        }

        try
        {
            _ = await client.GetValueAsync(
                new GetValueAsyncRequest { CacheName = "default", Key = "internal-owner-probe" },
                new CallOptions(headers, cancellationToken: cancellationToken)).ResponseAsync.ConfigureAwait(false);
            return StatusCode.OK;
        }
        catch (RpcException ex)
        {
            return ex.StatusCode;
        }
    }
}
