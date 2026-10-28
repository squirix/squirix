using System;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Invocation;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Maps domain errors to gRPC statuses, enters the remote-invocation scope and correlates inbound calls.</summary>
internal sealed class InboundCallInterceptor : Interceptor
{
    private readonly ServerCallCorrelation _correlation;
    private readonly MtlsOptions _mtlsOptions;
    private readonly VerifiedPeerCertificates? _peers;
    private readonly RingAgreement _ringAgreement;
    private readonly IRemoteInvocationScopeFactory _scopeFactory;

    internal InboundCallInterceptor(
        IRemoteInvocationScopeFactory scopeFactory,
        MtlsOptions mtlsOptions,
        VerifiedPeerCertificates? peers,
        RingAgreement ringAgreement,
        ServerCallCorrelation correlation)
    {
        ArgumentNullException.ThrowIfNull(scopeFactory);
        ArgumentNullException.ThrowIfNull(mtlsOptions);
        ArgumentNullException.ThrowIfNull(ringAgreement);
        ArgumentNullException.ThrowIfNull(correlation);
        _scopeFactory = scopeFactory;
        _mtlsOptions = mtlsOptions;
        _peers = peers;
        _ringAgreement = ringAgreement;
        _correlation = correlation;
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
            using var activity = ServerCallCorrelation.StartActivity(context);
            using var logScope = _correlation.BeginLogScope(context);
            using var deadlineScope = ServerCallCorrelation.PushDeadline(context);
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

    private bool ResolveInternalOwnerInvocation(ServerCallContext context)
    {
        if (!SquirixClusterConnectionSecurity.IsInternalOwnerHeaderPresent(context))
            return false;

        if (!SquirixClusterConnectionSecurity.TryGetTrustedPeerNodeId(context, _mtlsOptions, _peers, out var peerNodeId))
            throw new RpcException(new Status(StatusCode.Unauthenticated, "Internal cluster invocation requires trusted peer mTLS."));

        // Refuse a caller with a different ring before any handler or idempotency store runs.
        var peerFingerprint = context.RequestHeaders.GetValue(RemoteInvocationContract.RingFingerprintHeaderName);
        _ringAgreement.EnsureInboundAgreement(peerFingerprint, peerNodeId);
        return true;
    }

    /// <summary>Centralizes trusted cluster-peer checks used by transport auth and inbound RPC classification.</summary>
    private static class SquirixClusterConnectionSecurity
    {
        internal static bool IsInternalOwnerHeaderPresent(ServerCallContext context)
        {
            var value = context.RequestHeaders.GetValue(RemoteInvocationContract.InternalOwnerRpcHeaderName);
            return string.Equals(value, RemoteInvocationContract.InternalOwnerRpcHeaderValue, StringComparison.Ordinal);
        }

        internal static bool TryGetTrustedPeerNodeId(ServerCallContext context, MtlsOptions mtlsOptions, VerifiedPeerCertificates? peers, out string nodeId)
        {
            ArgumentNullException.ThrowIfNull(context);
            ArgumentNullException.ThrowIfNull(mtlsOptions);

            nodeId = string.Empty;
            if (peers == null || mtlsOptions.InternalListenPort <= 0)
                return false;

            var httpContext = context.GetHttpContext();
            return InternalListenerConnection.ArrivedOnInternalPort(httpContext, mtlsOptions.InternalListenPort) &&
                   httpContext.Connection.ClientCertificate is { } certificate && peers.TryVerify(certificate, out nodeId);
        }
    }
}
