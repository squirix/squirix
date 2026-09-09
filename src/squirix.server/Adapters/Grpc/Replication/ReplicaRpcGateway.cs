using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Adapters.Grpc.Replication;

/// <summary>Owner-side replication RPCs over pooled internode channels.</summary>
[Immutable]
internal sealed class ReplicaRpcGateway : IReplicaRpcGateway
{
    private readonly IServerClientPool _pool;

    /// <summary>Initializes a new instance of the <see cref="ReplicaRpcGateway" /> class.</summary>
    /// <param name="pool">Pooled internode channels keyed by node identifier.</param>
    internal ReplicaRpcGateway(IServerClientPool pool)
    {
        ArgumentNullException.ThrowIfNull(pool);
        _pool = pool;
    }

    /// <inheritdoc />
    public async Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        var client = new SquirixReplicationService.SquirixReplicationServiceClient(_pool.OpenChannel(nodeId));
        var response = await client.AppendReplicaEntriesAsync(MapRequest(header, batch), cancellationToken: cancellationToken).ResponseAsync.ConfigureAwait(false);
        return new FollowerLogAppendResult(response.Success, response.RefusalCode, response.Term, response.LastLogIndex);
    }

    private static ReplicaLogEntry MapEntry(in ReplicaLogRecord record) => new()
    {
        LogIndex = record.LogIndex,
        Term = record.Term,
        OperationId = record.OperationId,
        OperationScope = record.OperationScope,
        OperationFingerprint = ByteString.CopyFrom(record.OperationFingerprint.Span),
        RecordKind = record.RecordKind,
        CacheName = record.CacheName,
        KeyPayload = ByteString.CopyFrom(record.KeyPayload.Span),
        MutationKind = record.MutationKind,
        MutationPayload = ByteString.CopyFrom(record.MutationPayload.Span),
        OutcomePayload = ByteString.CopyFrom(record.OutcomePayload.Span),
        ExpiresUtcTicks = record.ExpiresUtcTicks,
        CreatedUtcTicks = record.CreatedUtcTicks,
        ResolvedUtcTicks = record.ResolvedUtcTicks,
        PayloadChecksum = record.PayloadChecksum,
    };

    private static ReplicationEnvelopeHeader MapHeader(ReplicaRpcHeader header) => new()
    {
        SchemaVersion = EnvelopeCodec.SchemaVersion,
        GroupId = header.GroupId,
        TopologyFingerprint = ByteString.CopyFrom(header.TopologyFingerprint.Span),
        ConfigurationGeneration = header.ConfigurationGeneration,
        Term = header.Term,
        LeaderNodeId = header.LeaderNodeId,
        SenderNodeId = header.SenderNodeId,
    };

    private static AppendReplicaEntriesRequest MapRequest(ReplicaRpcHeader header, FollowerBatch batch)
    {
        var request = new AppendReplicaEntriesRequest
        {
            Header = MapHeader(header),
            PrevLogIndex = batch.PrevLogIndex,
            PrevLogTerm = batch.PrevLogTerm,
            LeaderCommitIndex = batch.LeaderCommitIndex,
        };
        var records = batch.Records;
        for (var i = 0; i < records.Count; i++)
            request.Entries.Add(MapEntry(records[i]));

        return request;
    }
}
