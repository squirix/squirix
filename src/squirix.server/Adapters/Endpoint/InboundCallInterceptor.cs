using System;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Errors;
using Squirix.Server.Runtime.Invocation;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Maps domain errors to gRPC statuses and enters the remote-invocation scope for inbound calls.</summary>
internal sealed class InboundCallInterceptor : Interceptor
{
    private readonly MtlsOptions _mtlsOptions;
    private readonly VerifiedPeerCertificates? _peers;
    private readonly RingAgreement _ringAgreement;
    private readonly IRemoteInvocationScopeFactory _scopeFactory;

    internal InboundCallInterceptor(
        IRemoteInvocationScopeFactory scopeFactory,
        TopologyOptions cluster,
        MtlsOptions mtlsOptions,
        MtlsCertificate mtls,
        RingAgreement ringAgreement)
    {
        ArgumentNullException.ThrowIfNull(scopeFactory);
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentNullException.ThrowIfNull(mtlsOptions);
        ArgumentNullException.ThrowIfNull(mtls);
        ArgumentNullException.ThrowIfNull(ringAgreement);
        _scopeFactory = scopeFactory;
        _mtlsOptions = mtlsOptions;
        _peers = mtls is { Enabled: true, PeerChains: { } peerChains }
            ? new VerifiedPeerCertificates(peerChains, MtlsTopology.GetRemotePeerNodeIds(cluster), TimeProvider.System)
            : null;
        _ringAgreement = ringAgreement;
    }

    public override async Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        using var scope = _scopeFactory.EnterRemoteInvocation(ResolveInternalOwnerInvocation(context));
        await continuation(request, responseStream, context).ConfigureAwait(false);
    }

    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        try
        {
            using var scope = _scopeFactory.EnterRemoteInvocation(ResolveInternalOwnerInvocation(context));
            return await continuation(request, context).ConfigureAwait(false);
        }
        catch (ResourceExhaustedException ex)
        {
            throw ex.ToRpcException();
        }
        catch (JournalCapacityExceededException ex)
        {
            throw ex.ToRpcException();
        }
        catch (ServerOpIdMismatchException)
        {
            throw ServerOpContract.OperationIdReuseMismatch().ToRpcException();
        }
        catch (SquirixException ex)
        {
            throw ex.ToRpcException();
        }
    }

    private static string ResolvePeerNodeId(ServerCallContext context)
    {
        var certificate = context.GetHttpContext().Connection.ClientCertificate;
        return certificate != null && MtlsCertificateIdentity.TryGetNodeId(certificate, out var nodeId) ? nodeId : "unknown";
    }

    private bool ResolveInternalOwnerInvocation(ServerCallContext context)
    {
        SquirixClusterConnectionSecurity.RejectSpoofedInternalOwnerHeader(context, _mtlsOptions, _peers);
        if (!SquirixClusterConnectionSecurity.IsTrustedInternalOwnerCall(context, _mtlsOptions, _peers))
            return false;

        // Refuse a caller with a different ring before any handler or idempotency store runs.
        var peerFingerprint = context.RequestHeaders.GetValue(RemoteInvocationContract.RingFingerprintHeaderName);
        _ringAgreement.EnsureInboundAgreement(peerFingerprint, ResolvePeerNodeId(context));
        return true;
    }

    /// <summary>Centralizes trusted cluster-peer checks used by transport auth and inbound RPC classification.</summary>
    private static class SquirixClusterConnectionSecurity
    {
        internal static bool IsTrustedInternalOwnerCall(ServerCallContext context, MtlsOptions mtlsOptions, VerifiedPeerCertificates? peers)
        {
            ArgumentNullException.ThrowIfNull(context);
            ArgumentNullException.ThrowIfNull(mtlsOptions);

            if (peers == null || mtlsOptions.InternalListenPort <= 0)
                return false;

            var httpContext = context.GetHttpContext();
            return httpContext.Connection.LocalPort == mtlsOptions.InternalListenPort && IsInternalOwnerHeaderPresent(context) &&
                   httpContext.Connection.ClientCertificate is { } certificate && peers.TryVerify(certificate, out _);
        }

        internal static void RejectSpoofedInternalOwnerHeader(ServerCallContext context, MtlsOptions mtlsOptions, VerifiedPeerCertificates? peers)
        {
            ArgumentNullException.ThrowIfNull(context);
            ArgumentNullException.ThrowIfNull(mtlsOptions);

            if (!IsInternalOwnerHeaderPresent(context) || IsTrustedInternalOwnerCall(context, mtlsOptions, peers))
                return;

            throw new RpcException(new Status(StatusCode.Unauthenticated, "Internal cluster invocation requires trusted peer mTLS."));
        }

        private static bool IsInternalOwnerHeaderPresent(ServerCallContext context)
        {
            var value = context.RequestHeaders.GetValue(RemoteInvocationContract.InternalOwnerRpcHeaderName);
            return string.Equals(value, RemoteInvocationContract.InternalOwnerRpcHeaderValue, StringComparison.Ordinal);
        }
    }
}
