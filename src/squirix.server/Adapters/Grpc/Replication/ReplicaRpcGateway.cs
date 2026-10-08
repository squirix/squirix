using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Adapters.Grpc.Replication;

/// <summary>Owner-side replication RPCs and candidate-side election RPCs over pooled internode channels.</summary>
[Immutable]
internal sealed class ReplicaRpcGateway : IReplicaRpcGateway, IReplicaVoteGateway
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
        using var lease = _pool.LeaseChannel(nodeId, cancellationToken);
        var client = new SquirixReplicationService.SquirixReplicationServiceClient(lease.Channel);
        var response = await client.AppendReplicaEntriesAsync(MapRequest(in header, in batch), cancellationToken: lease.Token).ResponseAsync.ConfigureAwait(false);
        return new FollowerLogAppendResult(response.Success, response.RefusalCode, response.Term, response.LastLogIndex);
    }

    /// <inheritdoc />
    public async Task<FollowerLogVoteResult> PreVoteAsync(string nodeId, ReplicaRpcHeader header, ulong lastLogIndex, ulong lastLogTerm, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        using var lease = _pool.LeaseChannel(nodeId, cancellationToken);
        var client = new SquirixReplicationService.SquirixReplicationServiceClient(lease.Channel);
        var response = await client.PreVoteAsync(MapVoteRequest(in header, lastLogIndex, lastLogTerm), cancellationToken: lease.Token).ResponseAsync.ConfigureAwait(false);
        return new FollowerLogVoteResult(response.Granted, response.RefusalCode, response.Term);
    }

    /// <inheritdoc />
    public async Task<FollowerLogVoteResult> RequestVoteAsync(string nodeId, ReplicaRpcHeader header, ulong lastLogIndex, ulong lastLogTerm, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        using var lease = _pool.LeaseChannel(nodeId, cancellationToken);
        var client = new SquirixReplicationService.SquirixReplicationServiceClient(lease.Channel);
        var response = await client.RequestVoteAsync(MapVoteRequest(in header, lastLogIndex, lastLogTerm), cancellationToken: lease.Token).ResponseAsync.ConfigureAwait(false);
        return new FollowerLogVoteResult(response.Granted, response.RefusalCode, response.Term);
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
        DecidedUtcTicks = record.DecidedUtcTicks,
        ResolvedUtcTicks = record.ResolvedUtcTicks,
        PayloadChecksum = record.PayloadChecksum,
    };

    private static ReplicationEnvelopeHeader MapHeader(in ReplicaRpcHeader header) => new()
    {
        SchemaVersion = EnvelopeSchema.Version,
        GroupId = header.GroupId,
        TopologyFingerprint = ByteString.CopyFrom(header.TopologyFingerprint.Span),
        ConfigurationGeneration = header.ConfigurationGeneration,
        Term = header.Term,
        LeaderNodeId = header.LeaderNodeId,
        SenderNodeId = header.SenderNodeId,
    };

    private static AppendReplicaEntriesRequest MapRequest(in ReplicaRpcHeader header, in FollowerBatch batch)
    {
        var request = new AppendReplicaEntriesRequest
        {
            Header = MapHeader(in header),
            PrevLogIndex = batch.PrevLogIndex,
            PrevLogTerm = batch.PrevLogTerm,
            LeaderCommitIndex = batch.LeaderCommitIndex,
        };
        var records = batch.Records;
        for (var i = 0; i < records.Count; i++)
            request.Entries.Add(MapEntry(records[i]));

        return request;
    }

    private static ReplicaVoteRequest MapVoteRequest(in ReplicaRpcHeader header, ulong lastLogIndex, ulong lastLogTerm)
    {
        // A candidate claims no leadership: the voter ignores the leader identity, so none is sent.
        var envelope = MapHeader(in header);
        envelope.LeaderNodeId = string.Empty;
        return new ReplicaVoteRequest
        {
            Header = envelope,
            LastLogIndex = lastLogIndex,
            LastLogTerm = lastLogTerm,
        };
    }
}
