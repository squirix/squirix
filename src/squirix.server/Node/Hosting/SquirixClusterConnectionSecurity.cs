using System;
using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.Hosting;

/// <summary>
/// Centralizes trusted cluster-peer checks used by transport auth and inbound RPC classification.
/// </summary>
internal static class SquirixClusterConnectionSecurity
{
    public static bool IsTrustedInternalOwnerCall(ServerCallContext context, ClusterConfig cluster, MtlsOptions mtlsOptions, MtlsCertificateMaterial mtlsMaterial)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentNullException.ThrowIfNull(mtlsOptions);
        ArgumentNullException.ThrowIfNull(mtlsMaterial);

        if (!mtlsMaterial.Enabled || mtlsOptions.InternalListenPort <= 0)
            return false;

        var httpContext = context.GetHttpContext();
        return httpContext.Connection.LocalPort == mtlsOptions.InternalListenPort && IsInternalOwnerHeaderPresent(context) &&
               IsTrustedClusterPeer(httpContext, cluster, mtlsMaterial);
    }

    public static void RejectSpoofedInternalOwnerHeader(ServerCallContext context, ClusterConfig cluster, MtlsOptions mtlsOptions, MtlsCertificateMaterial mtlsMaterial)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentNullException.ThrowIfNull(mtlsOptions);
        ArgumentNullException.ThrowIfNull(mtlsMaterial);

        if (!IsInternalOwnerHeaderPresent(context) || IsTrustedInternalOwnerCall(context, cluster, mtlsOptions, mtlsMaterial))
            return;

        throw new RpcException(new Status(StatusCode.Unauthenticated, "Internal cluster invocation requires trusted peer mTLS."));
    }

    private static bool IsInternalOwnerHeaderPresent(ServerCallContext context)
    {
        foreach (var header in context.RequestHeaders)
        {
            if (!string.Equals(header.Key, RemoteInvocationContract.InternalOwnerRpcHeaderName, StringComparison.OrdinalIgnoreCase))
                continue;

            return string.Equals(header.Value, RemoteInvocationContract.InternalOwnerRpcHeaderValue, StringComparison.Ordinal);
        }

        return false;
    }

    private static bool IsTrustedClusterPeer(HttpContext httpContext, ClusterConfig cluster, MtlsCertificateMaterial mtlsMaterial)
    {
        if (!mtlsMaterial.Enabled || mtlsMaterial.TrustAnchor is null)
            return false;

        var certificate = httpContext.Connection.ClientCertificate;
        return MtlsClientCertificateValidator.ValidateForConfiguredRemotePeer(certificate, mtlsMaterial.TrustAnchor, MtlsTopology.GetRemotePeerNodeIds(cluster));
    }
}
