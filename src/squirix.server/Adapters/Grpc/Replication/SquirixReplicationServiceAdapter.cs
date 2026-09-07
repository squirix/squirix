using System;
using System.Buffers;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Utils;

namespace Squirix.Server.Adapters.Grpc.Replication;

/// <summary>Closed replication gRPC adapter. Identity-checked; without a group registry it stays a refusing stub.</summary>
[Immutable]
internal sealed class SquirixReplicationServiceAdapter : SquirixReplicationService.SquirixReplicationServiceBase
{
    private readonly ulong _configurationGeneration;
    private readonly ReplicaFollower? _follower;
    private readonly MtlsCertificateMaterial _mtlsMaterial;
    private readonly MtlsOptions _mtlsOptions;
    private readonly string[] _remotePeerNodeIds;
    private readonly TopologyFingerprint _topologyFingerprint;

    internal SquirixReplicationServiceAdapter(TopologyOptions cluster, MtlsOptions mtlsOptions, MtlsCertificateMaterial mtlsMaterial, ReplicaGroupRegistry? groups = null)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentNullException.ThrowIfNull(mtlsOptions);
        ArgumentNullException.ThrowIfNull(mtlsMaterial);
        _mtlsOptions = mtlsOptions;
        _mtlsMaterial = mtlsMaterial;
        _remotePeerNodeIds = MtlsTopology.GetRemotePeerNodeIds(cluster);
        _follower = groups == null ? null : new ReplicaFollower(groups);

        _topologyFingerprint = TopologyFingerprint.CreateFromTopology(cluster, _mtlsOptions);
        _configurationGeneration = cluster.ConfigurationGeneration;
    }

    public override async Task<AdvanceReplicaCommitResponse> AdvanceReplicaCommit(AdvanceReplicaCommitRequest request, ServerCallContext context)
    {
        var header = EnsureHeader(request.Header, context, true);
        if (_follower == null)
            return StubCommitRefusal(header);

        var result = await CommitReplicaAdvanceAsync(_follower, header, request.CommitIndex, context.CancellationToken).ConfigureAwait(false);
        var status = await _follower.GetStatusAsync(header.GroupId, context.CancellationToken).ConfigureAwait(false);
        return new AdvanceReplicaCommitResponse
        {
            Term = status?.CurrentTerm ?? 0,
            CommitIndex = result.CommitIndex,
            Success = result.Success,
            RefusalCode = result.RefusalCode,
        };
    }

    public override async Task<AppendReplicaEntriesResponse> AppendReplicaEntries(AppendReplicaEntriesRequest request, ServerCallContext context)
    {
        var header = EnsureHeader(request.Header, context, true);
        if (_follower == null)
            return StubAppendRefusal(header);

        var batch = BuildFollowerBatch(request, header);
        var result = await _follower.AppendAsync(header.GroupId, header.TopologyFingerprint.ToByteArray(), header.ConfigurationGeneration, batch, context.CancellationToken)
                                    .ConfigureAwait(false);
        return new AppendReplicaEntriesResponse
        {
            Term = result.CurrentTerm,
            LastLogIndex = result.LastLogIndex,
            Success = result.Success,
            RefusalCode = result.RefusalCode,
        };
    }

    public override async Task<GetReplicaStatusResponse> GetReplicaStatus(GetReplicaStatusRequest request, ServerCallContext context)
    {
        var header = EnsureHeader(request.Header, context, false);
        var status = _follower == null ? null : await _follower.GetStatusAsync(header.GroupId, context.CancellationToken).ConfigureAwait(false);
        if (status == null)
        {
            return new GetReplicaStatusResponse
            {
                Term = header.Term,

                // When refusing to report status, keep the stub shape: an explicit unknown readiness
                // rather than conflating with the refusal code. Tests assert RefusalCode separately.
                Role = "follower",
                LastLogIndex = 0,
                CommitIndex = 0,
                Readiness = "unknown",
                TopologyFingerprint = ByteString.CopyFrom(_topologyFingerprint.Bytes),
                ConfigurationGeneration = _configurationGeneration,
                RefusalCode = RefusalCodes.NotReady,
            };
        }

        var current = status.Value;
        return new GetReplicaStatusResponse
        {
            Term = current.CurrentTerm,
            Role = "follower",
            LastLogIndex = current.LastLogIndex,
            CommitIndex = current.CommitIndex,
            Readiness = MapReadiness(current.Readiness),
            TopologyFingerprint = ByteString.CopyFrom(current.TopologyFingerprint.ToArray()),
            ConfigurationGeneration = current.ConfigurationGeneration,
            RefusalCode = string.Empty,
        };
    }

    public override async Task<InstallReplicaSnapshotResponse> InstallReplicaSnapshot(IAsyncStreamReader<InstallReplicaSnapshotRequest> requestStream, ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(requestStream);

        if (!await requestStream.MoveNext(context.CancellationToken).ConfigureAwait(false))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "InstallReplicaSnapshot requires at least one chunk."));

        var header = EnsureHeader(requestStream.Current.Header, context, true);
        var first = requestStream.Current;
        if (_follower == null)
            return await DrainStubInstallAsync(requestStream, header, context.CancellationToken).ConfigureAwait(false);

        var (result, status) = await ReadAndInstallSnapshotAsync(_follower, requestStream, first, header, context.CancellationToken).ConfigureAwait(false);
        return new InstallReplicaSnapshotResponse
        {
            Term = status?.CurrentTerm ?? 0,
            Success = result.Success,
            RefusalCode = result.Refusal,
        };
    }

    /// <summary>Accumulates the snapshot chunks and drives the follower install.</summary>
    /// <param name="follower">The active follower, guaranteed non-null by the caller.</param>
    /// <param name="requestStream">The snapshot chunk stream.</param>
    /// <param name="first">The first chunk, already validated as present.</param>
    /// <param name="header">Validated replication envelope identity.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The install outcome and the follower status after the install.</returns>
    /// <exception cref="RpcException">Thrown when the chunk stream length differs from the declared total bytes.</exception>
    private static async Task<(GroupSnapshotInstallResult Result, FollowerLogStatus? Status)> ReadAndInstallSnapshotAsync(
        ReplicaFollower follower,
        IAsyncStreamReader<InstallReplicaSnapshotRequest> requestStream,
        InstallReplicaSnapshotRequest first,
        ReplicationEnvelopeHeader header,
        CancellationToken cancellationToken)
    {
        var file = new ArrayBufferWriter<byte>();
        AccumulateChunk(file, first, header);
        while (await requestStream.MoveNext(cancellationToken).ConfigureAwait(false))
            AccumulateChunk(file, requestStream.Current, header);

        if (ulong.CreateChecked(file.WrittenCount) != first.TotalBytes)
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Snapshot chunk stream length differs from the declared total bytes."));

        var upload = new ReplicaSnapshotUpload(file.WrittenMemory, first.PayloadChecksum, first.LastIncludedIndex, first.LastIncludedTerm);
        var result = await follower.InstallSnapshotUploadAsync(
            header.GroupId,
            header.TopologyFingerprint.ToByteArray(),
            header.ConfigurationGeneration,
            upload,
            header.Term,
            cancellationToken).ConfigureAwait(false);
        var status = await follower.GetStatusAsync(header.GroupId, cancellationToken).ConfigureAwait(false);
        return (result, status);
    }

    private static void AccumulateChunk(ArrayBufferWriter<byte> file, InstallReplicaSnapshotRequest chunk, ReplicationEnvelopeHeader header)
    {
        var chunkHeader = chunk.Header;
        if (chunkHeader != null)
        {
            if (!string.Equals(chunkHeader.SenderNodeId, header.SenderNodeId, StringComparison.Ordinal))
                throw new RpcException(new Status(StatusCode.InvalidArgument, "Snapshot chunk header SenderNodeId differs from the first chunk."));

            if (!string.Equals(chunkHeader.LeaderNodeId, header.LeaderNodeId, StringComparison.Ordinal))
                throw new RpcException(new Status(StatusCode.InvalidArgument, "Snapshot chunk header LeaderNodeId differs from the first chunk."));

            if (!string.Equals(chunkHeader.GroupId, header.GroupId, StringComparison.Ordinal))
                throw new RpcException(new Status(StatusCode.InvalidArgument, "Snapshot chunk header GroupId differs from the first chunk."));
        }

        if (chunk.Chunk.IsEmpty)
            return;

        if (file.WrittenCount + chunk.Chunk.Length > GroupSnapshotStore.DefaultMaxSnapshotBytes)
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Snapshot chunk stream exceeds the maximum snapshot size."));

        file.Write(chunk.Chunk.Span);
    }

    /// <summary>Builds the follower append batch from a wire request.</summary>
    /// <param name="request">The append request.</param>
    /// <param name="header">Validated replication envelope identity.</param>
    /// <returns>The follower batch to append.</returns>
    private static FollowerBatch BuildFollowerBatch(AppendReplicaEntriesRequest request, ReplicationEnvelopeHeader header)
    {
        var records = new ReplicaLogRecord[request.Entries.Count];
        for (var i = 0; i < records.Length; i++)
            records[i] = MapRecord(request.Entries[i]);

        return new FollowerBatch(records, header.LeaderNodeId, header.Term, request.PrevLogIndex, request.PrevLogTerm, request.LeaderCommitIndex);
    }

    /// <summary>Advances the follower commit for a verified replication caller.</summary>
    /// <param name="follower">The active follower, guaranteed non-null by the caller.</param>
    /// <param name="header">Validated replication envelope identity.</param>
    /// <param name="commitIndex">Target commit index to advance to.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The follower commit advance result.</returns>
    private static Task<FollowerLogCommitResult> CommitReplicaAdvanceAsync(
        ReplicaFollower follower,
        ReplicationEnvelopeHeader header,
        ulong commitIndex,
        CancellationToken cancellationToken) => follower.AdvanceCommitAsync(
        header.GroupId,
        header.TopologyFingerprint.ToByteArray(),
        header.ConfigurationGeneration,
        commitIndex,
        header.Term,
        cancellationToken);

    private static async Task<InstallReplicaSnapshotResponse> DrainStubInstallAsync(
        IAsyncStreamReader<InstallReplicaSnapshotRequest> requestStream,
        ReplicationEnvelopeHeader header,
        CancellationToken cancellationToken)
    {
        while (await requestStream.MoveNext(cancellationToken).ConfigureAwait(false))
        {
            var currentHeader = requestStream.Current.Header;
            if (currentHeader == null)
                continue;

            if (!string.Equals(currentHeader.SenderNodeId, header.SenderNodeId, StringComparison.Ordinal))
                throw new RpcException(new Status(StatusCode.InvalidArgument, "Snapshot chunk header SenderNodeId differs from the first chunk."));

            if (!string.Equals(currentHeader.LeaderNodeId, header.LeaderNodeId, StringComparison.Ordinal))
                throw new RpcException(new Status(StatusCode.InvalidArgument, "Snapshot chunk header LeaderNodeId differs from the first chunk."));

            if (!string.Equals(currentHeader.GroupId, header.GroupId, StringComparison.Ordinal))
                throw new RpcException(new Status(StatusCode.InvalidArgument, "Snapshot chunk header GroupId differs from the first chunk."));
        }

        return new InstallReplicaSnapshotResponse
        {
            Term = header.Term,
            Success = false,
            RefusalCode = RefusalCodes.NotReady,
        };
    }

    private static string MapReadiness(FollowerLogReadiness readiness) => readiness switch
    {
        FollowerLogReadiness.Ready => "ready",
        FollowerLogReadiness.Failed => "failed",
        FollowerLogReadiness.Unknown => "unknown",
        _ => throw new ArgumentOutOfRangeException(nameof(readiness), readiness, "Unsupported follower readiness."),
    };

    private static ReplicaLogRecord MapRecord(ReplicaLogEntry entry) => new(
        entry.LogIndex,
        entry.Term,
        entry.OperationId,
        entry.OperationScope,
        entry.OperationFingerprint.ToByteArray(),
        entry.RecordKind,
        entry.CacheName,
        entry.KeyPayload.ToByteArray(),
        entry.MutationKind,
        entry.MutationPayload.ToByteArray(),
        entry.OutcomePayload.ToByteArray(),
        entry.ExpiresUtcTicks,
        entry.CreatedUtcTicks,
        entry.ResolvedUtcTicks,
        entry.PayloadChecksum);

    private static AppendReplicaEntriesResponse StubAppendRefusal(ReplicationEnvelopeHeader header) => new()
    {
        Term = header.Term,

        // When refusing to append entries, report the follower's last log index as a conflict hint. This stub follower has no log; return 0 rather than
        // echoing the leader's PrevLogIndex which would mislead the leader.
        LastLogIndex = 0,
        Success = false,
        RefusalCode = RefusalCodes.NotReady,
    };

    private static AdvanceReplicaCommitResponse StubCommitRefusal(ReplicationEnvelopeHeader header) => new()
    {
        Term = header.Term,

        // When refusing to advance the commit, report the follower's actual commit index. This stub follower has no committed log; return 0
        // rather than echoing the leader's CommitIndex which would mislead the leader.
        CommitIndex = 0,
        Success = false,
        RefusalCode = RefusalCodes.NotReady,
    };

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

            var certificate = httpContext.Connection.ClientCertificate ?? ThrowHelper.Throw<X509Certificate2>(
                new RpcException(new Status(StatusCode.Unauthenticated, "Replication requires a trusted peer client certificate.")));

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
