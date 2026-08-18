using System;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Squirix.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;

namespace Squirix.Server.Adapters.Grpc.Replication;

/// <summary>Closed replication gRPC adapter. Identity-checked; durable follow-up lands in later M8 tasks.</summary>
[Immutable]
internal sealed class SquirixReplicationServiceAdapter : SquirixReplicationService.SquirixReplicationServiceBase
{
    private readonly ulong _configurationGeneration;
    private readonly MtlsCertificateMaterial _mtlsMaterial;
    private readonly MtlsOptions _mtlsOptions;
    private readonly string[] _remotePeerNodeIds;
    private readonly TopologyFingerprint _topologyFingerprint;

    internal SquirixReplicationServiceAdapter(TopologyOptions cluster, MtlsOptions mtlsOptions, MtlsCertificateMaterial mtlsMaterial)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        _mtlsOptions = mtlsOptions ?? throw new ArgumentNullException(nameof(mtlsOptions));
        _mtlsMaterial = mtlsMaterial ?? throw new ArgumentNullException(nameof(mtlsMaterial));
        _remotePeerNodeIds = MtlsTopology.GetRemotePeerNodeIds(cluster);

        _topologyFingerprint = TopologyFingerprint.CreateFromTopology(cluster, _mtlsOptions);
        _configurationGeneration = cluster.ConfigurationGeneration;
    }

    public override Task<AdvanceReplicaCommitResponse> AdvanceReplicaCommit(AdvanceReplicaCommitRequest request, ServerCallContext context)
    {
        var header = EnsureHeader(request.Header, context, true);
        var result = new AdvanceReplicaCommitResponse
        {
            Term = header.Term,

            // When refusing to advance the commit, report the follower's actual commit index. This stub follower has no committed log; return 0
            // rather than echoing the leader's CommitIndex which would mislead the leader.
            CommitIndex = 0,
            Success = false,
            RefusalCode = RefusalCodes.NotReady,
        };
        return Task.FromResult(result);
    }

    public override Task<AppendReplicaEntriesResponse> AppendReplicaEntries(AppendReplicaEntriesRequest request, ServerCallContext context)
    {
        var header = EnsureHeader(request.Header, context, true);
        var result = new AppendReplicaEntriesResponse
        {
            Term = header.Term,

            // When refusing to append entries, report the follower's last log index as a conflict hint. This stub follower has no log; return 0 rather than
            // echoing the leader's PrevLogIndex which would mislead the leader.
            LastLogIndex = 0,
            Success = false,
            RefusalCode = RefusalCodes.NotReady,
        };
        return Task.FromResult(result);
    }

    public override Task<GetReplicaStatusResponse> GetReplicaStatus(GetReplicaStatusRequest request, ServerCallContext context)
    {
        var header = EnsureHeader(request.Header, context, false);
        var response = new GetReplicaStatusResponse
        {
            Term = header.Term,
            Role = "follower",
            LastLogIndex = 0,
            CommitIndex = 0,

            // Report an explicit readiness state for the node. This stub node is not yet serving; use a distinct readiness marker rather than conflating
            // with the refusal code. Tests assert RefusalCode separately.
            Readiness = "unknown",
            TopologyFingerprint = ByteString.CopyFrom(_topologyFingerprint.Bytes),
            ConfigurationGeneration = _configurationGeneration,
            RefusalCode = RefusalCodes.NotReady,
        };
        return Task.FromResult(response);
    }

    public override async Task<InstallReplicaSnapshotResponse> InstallReplicaSnapshot(IAsyncStreamReader<InstallReplicaSnapshotRequest> requestStream, ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(requestStream);

        if (!await requestStream.MoveNext(context.CancellationToken).ConfigureAwait(false))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "InstallReplicaSnapshot requires at least one chunk."));

        var header = EnsureHeader(requestStream.Current.Header, context, true);
        while (await requestStream.MoveNext(context.CancellationToken).ConfigureAwait(false))
        {
            var currentHeader = requestStream.Current.Header;
            if (currentHeader == null)
                continue;

            if (!string.Equals(currentHeader.SenderNodeId, header.SenderNodeId, StringComparison.Ordinal))
                throw new RpcException(new Status(StatusCode.InvalidArgument, "Snapshot chunk header SenderNodeId differs from the first chunk."));

            if (!string.Equals(currentHeader.LeaderNodeId, header.LeaderNodeId, StringComparison.Ordinal))
                throw new RpcException(new Status(StatusCode.InvalidArgument, "Snapshot chunk header LeaderNodeId differs from the first chunk."));
        }

        return new InstallReplicaSnapshotResponse
        {
            Term = header.Term,
            Success = false,
            RefusalCode = RefusalCodes.NotReady,
        };
    }

    private ReplicationEnvelopeHeader EnsureHeader(ReplicationEnvelopeHeader? header, ServerCallContext context, bool requireLeader)
    {
        if (header == null || string.IsNullOrWhiteSpace(header.SenderNodeId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Replication envelope header with sender_node_id is required."));

        if (requireLeader && string.IsNullOrWhiteSpace(header.LeaderNodeId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Replication envelope header with leader_node_id is required."));

        _ = PeerAuth.EnsureTrustedPeer(context, _mtlsOptions, _mtlsMaterial, _remotePeerNodeIds, header.SenderNodeId, requireLeader ? header.LeaderNodeId : null);

        if (header.SchemaVersion != EnvelopeCodec.SchemaVersion)
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Unsupported replication envelope schema version."));

        return header;
    }

    /// <summary>Enforces internal-listener + mTLS NodeId binding for closed replication RPCs.</summary>
    private static class PeerAuth
    {
        /// <summary>
        /// Ensures the call arrived on the internal mTLS listener with a peer certificate whose NodeId is in
        /// <see cref="TopologyOptions.Peers" /> and matches <paramref name="claimedSenderNodeId" />. When
        /// <paramref name="claimedLeaderNodeId" /> is supplied, also binds the claimed leader identity to the same
        /// certificate. Host-header spoofing is ignored; <see cref="ConnectionInfo.LocalPort" /> is authoritative.
        /// </summary>
        /// <param name="context">gRPC server call context.</param>
        /// <param name="mtlsOptions">Cluster mTLS options.</param>
        /// <param name="mtlsMaterial">Loaded cluster mTLS material.</param>
        /// <param name="remotePeerNodeIds">Configured remote peer node identifiers for inbound certificate checks.</param>
        /// <param name="claimedSenderNodeId">Sender node id claimed by the request envelope.</param>
        /// <param name="claimedLeaderNodeId">Leader node id claimed by the request envelope; null when the operation is not leader-authorized.</param>
        /// <returns>Validated peer node id from the client certificate.</returns>
        /// <exception cref="RpcException">Thrown when the call is not a trusted internal replication peer.</exception>
        internal static string EnsureTrustedPeer(
            ServerCallContext context,
            MtlsOptions mtlsOptions,
            MtlsCertificateMaterial mtlsMaterial,
            string[] remotePeerNodeIds,
            string claimedSenderNodeId,
            string? claimedLeaderNodeId = null)
        {
            ArgumentNullException.ThrowIfNull(context);
            ArgumentNullException.ThrowIfNull(mtlsOptions);
            ArgumentNullException.ThrowIfNull(mtlsMaterial);
            ArgumentNullException.ThrowIfNull(remotePeerNodeIds);
            ArgumentException.ThrowIfNullOrWhiteSpace(claimedSenderNodeId);

            if (!mtlsMaterial.Enabled || mtlsOptions.InternalListenPort <= 0 || mtlsMaterial.TrustAnchor == null)
                throw new RpcException(new Status(StatusCode.Unavailable, "Internal replication listener is not configured."));

            var httpContext = context.GetHttpContext();
            if (httpContext.Connection.LocalPort != mtlsOptions.InternalListenPort)
                throw new RpcException(new Status(StatusCode.PermissionDenied, "Replication service is bound to the internal mTLS listener only."));

            var certificate = httpContext.Connection.ClientCertificate ??
                              throw new RpcException(new Status(StatusCode.Unauthenticated, "Replication requires a trusted peer client certificate."));

            if (!MtlsClientCertificateValidator.ValidateForConfiguredRemotePeer(certificate, mtlsMaterial.TrustAnchor, remotePeerNodeIds))
                throw new RpcException(new Status(StatusCode.Unauthenticated, "Replication peer certificate is not a configured cluster member."));

            if (!MtlsCertificateIdentity.TryGetNodeId(certificate, out var certificateNodeId))
                throw new RpcException(new Status(StatusCode.Unauthenticated, "Replication peer certificate is missing a NodeId identity."));

            if (!string.Equals(certificateNodeId, claimedSenderNodeId, StringComparison.Ordinal))
                throw new RpcException(new Status(StatusCode.Unauthenticated, "Replication sender_node_id does not match the peer certificate NodeId."));

            if (claimedLeaderNodeId != null && !string.Equals(certificateNodeId, claimedLeaderNodeId, StringComparison.Ordinal))
                throw new RpcException(new Status(StatusCode.PermissionDenied, "Replication leader_node_id does not match the peer certificate NodeId."));

            return certificateNodeId;
        }
    }
}
