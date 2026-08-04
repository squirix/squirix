using System;
using System.Threading.Tasks;
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

    public SquirixReplicationServiceAdapter(TopologyOptions cluster, MtlsOptions mtlsOptions, MtlsCertificateMaterial mtlsMaterial)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        _mtlsOptions = mtlsOptions ?? throw new ArgumentNullException(nameof(mtlsOptions));
        _mtlsMaterial = mtlsMaterial ?? throw new ArgumentNullException(nameof(mtlsMaterial));
        _remotePeerNodeIds = MtlsTopology.GetRemotePeerNodeIds(cluster);
    }

    public override Task<AdvanceReplicaCommitResponse> AdvanceReplicaCommit(AdvanceReplicaCommitRequest request, ServerCallContext context)
    {
        var header = EnsureHeader(request.Header, context);
        return Task.FromResult(
            new AdvanceReplicaCommitResponse
            {
                Term = header.Term,
                CommitIndex = request.CommitIndex,
                Success = false,
                RefusalCode = RefusalCodes.NotReady,
            });
    }

    public override Task<AppendReplicaEntriesResponse> AppendReplicaEntries(AppendReplicaEntriesRequest request, ServerCallContext context)
    {
        var header = EnsureHeader(request.Header, context);
        return Task.FromResult(
            new AppendReplicaEntriesResponse
            {
                Term = header.Term,
                LastLogIndex = request.PrevLogIndex,
                Success = false,
                RefusalCode = RefusalCodes.NotReady,
            });
    }

    public override Task<GetReplicaStatusResponse> GetReplicaStatus(GetReplicaStatusRequest request, ServerCallContext context)
    {
        var header = EnsureHeader(request.Header, context);
        return Task.FromResult(
            new GetReplicaStatusResponse
            {
                Term = header.Term,
                Role = "follower",
                LastLogIndex = 0,
                CommitIndex = 0,
                Readiness = RefusalCodes.NotReady,
                TopologyFingerprint = header.TopologyFingerprint,
                ConfigurationGeneration = header.ConfigurationGeneration,
                RefusalCode = RefusalCodes.NotReady,
            });
    }

    public override async Task<InstallReplicaSnapshotResponse> InstallReplicaSnapshot(IAsyncStreamReader<InstallReplicaSnapshotRequest> requestStream, ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(requestStream);

        if (!await requestStream.MoveNext(context.CancellationToken).ConfigureAwait(false))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "InstallReplicaSnapshot requires at least one chunk."));

        var header = EnsureHeader(requestStream.Current.Header, context);
        while (await requestStream.MoveNext(context.CancellationToken).ConfigureAwait(false))
        {
            var currentHeader = requestStream.Current.Header;
            if (currentHeader is null)
                continue;

            if (!string.Equals(currentHeader.SenderNodeId, header.SenderNodeId, StringComparison.Ordinal))
                throw new RpcException(new Status(StatusCode.InvalidArgument, "Snapshot chunk header SenderNodeId differs from the first chunk."));
        }

        return new InstallReplicaSnapshotResponse
        {
            Term = header.Term,
            Success = false,
            RefusalCode = RefusalCodes.NotReady,
        };
    }

    private ReplicationEnvelopeHeader EnsureHeader(ReplicationEnvelopeHeader? header, ServerCallContext context)
    {
        if (header is null || string.IsNullOrWhiteSpace(header.SenderNodeId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Replication envelope header with sender_node_id is required."));

        _ = PeerAuth.EnsureTrustedPeer(context, _mtlsOptions, _mtlsMaterial, _remotePeerNodeIds, header.SenderNodeId);

        if (header.SchemaVersion is not EnvelopeCodec.SchemaVersion)
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Unsupported replication envelope schema version."));

        return header;
    }
}
