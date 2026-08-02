using System;
using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;

namespace Squirix.Server.Adapters.Grpc.Replication;

/// <summary>Enforces internal-listener + mTLS NodeId binding for closed replication RPCs.</summary>
internal static class PeerAuth
{
    /// <summary>
    /// Ensures the call arrived on the internal mTLS listener with a peer certificate whose NodeId is in
    /// <see cref="TopologyOptions.Peers" /> and matches <paramref name="claimedSenderNodeId" />.
    /// Host-header spoofing is ignored; <see cref="ConnectionInfo.LocalPort" /> is authoritative.
    /// </summary>
    /// <param name="context">gRPC server call context.</param>
    /// <param name="cluster">Local cluster topology.</param>
    /// <param name="mtlsOptions">Cluster mTLS options.</param>
    /// <param name="mtlsMaterial">Loaded cluster mTLS material.</param>
    /// <param name="claimedSenderNodeId">Sender node id claimed by the request envelope.</param>
    /// <returns>Validated peer node id from the client certificate.</returns>
    /// <exception cref="RpcException">Thrown when the call is not a trusted internal replication peer.</exception>
    internal static string EnsureTrustedPeer(
        ServerCallContext context,
        TopologyOptions cluster,
        MtlsOptions mtlsOptions,
        MtlsCertificateMaterial mtlsMaterial,
        string claimedSenderNodeId)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentNullException.ThrowIfNull(mtlsOptions);
        ArgumentNullException.ThrowIfNull(mtlsMaterial);
        ArgumentException.ThrowIfNullOrWhiteSpace(claimedSenderNodeId);

        if (!mtlsMaterial.Enabled || mtlsOptions.InternalListenPort <= 0 || mtlsMaterial.TrustAnchor is null)
            throw new RpcException(new Status(StatusCode.Unavailable, "Internal replication listener is not configured."));

        var httpContext = context.GetHttpContext();
        if (httpContext.Connection.LocalPort != mtlsOptions.InternalListenPort)
            throw new RpcException(new Status(StatusCode.PermissionDenied, "Replication service is bound to the internal mTLS listener only."));

        var certificate = httpContext.Connection.ClientCertificate
                          ?? throw new RpcException(new Status(StatusCode.Unauthenticated, "Replication requires a trusted peer client certificate."));

        if (!MtlsClientCertificateValidator.ValidateForConfiguredRemotePeer(certificate, mtlsMaterial.TrustAnchor, MtlsTopology.GetRemotePeerNodeIds(cluster)))
            throw new RpcException(new Status(StatusCode.Unauthenticated, "Replication peer certificate is not a configured cluster member."));

        if (!MtlsCertificateIdentity.TryGetNodeId(certificate, out var certificateNodeId))
            throw new RpcException(new Status(StatusCode.Unauthenticated, "Replication peer certificate is missing a NodeId identity."));

        if (!string.Equals(certificateNodeId, claimedSenderNodeId, StringComparison.Ordinal))
            throw new RpcException(new Status(StatusCode.Unauthenticated, "Replication sender_node_id does not match the peer certificate NodeId."));

        return certificateNodeId;
    }
}
