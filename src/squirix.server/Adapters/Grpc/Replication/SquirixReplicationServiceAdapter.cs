using System;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;

namespace Squirix.Server.Adapters.Grpc.Replication;

/// <summary>Closed replication gRPC adapter. Identity-checked; durable follow-up lands in later M8 tasks.</summary>
internal sealed class SquirixReplicationServiceAdapter : SquirixReplicationService.SquirixReplicationServiceBase
{
    private readonly MtlsCertificateMaterial _mtlsMaterial;
    private readonly MtlsOptions _mtlsOptions;
    private readonly string[] _remotePeerNodeIds;
    private readonly TopologyFingerprint _topologyFingerprint;
    private readonly ulong _configurationGeneration;

    public SquirixReplicationServiceAdapter(TopologyOptions cluster, MtlsOptions mtlsOptions, MtlsCertificateMaterial mtlsMaterial)
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
            if (currentHeader is null)
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
        if (header is null || string.IsNullOrWhiteSpace(header.SenderNodeId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Replication envelope header with sender_node_id is required."));

        if (requireLeader && string.IsNullOrWhiteSpace(header.LeaderNodeId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Replication envelope header with leader_node_id is required."));

        _ = PeerAuth.EnsureTrustedPeer(context, _mtlsOptions, _mtlsMaterial, _remotePeerNodeIds, header.SenderNodeId, requireLeader ? header.LeaderNodeId : null);

        if (header.SchemaVersion is not EnvelopeCodec.SchemaVersion)
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Unsupported replication envelope schema version."));

        return header;
    }
}
