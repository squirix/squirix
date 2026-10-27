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
        return MapAppendResponse(response);
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

    /// <summary>Maps the replication envelope identity to its wire message.</summary>
    /// <param name="header">The envelope identity.</param>
    /// <returns>The wire header.</returns>
    internal static ReplicationEnvelopeHeader MapHeader(in ReplicaRpcHeader header) => new()
    {
        SchemaVersion = EnvelopeSchema.Version,
        GroupId = header.GroupId,
        TopologyFingerprint = ByteString.CopyFrom(header.TopologyFingerprint.Span),
        ConfigurationGeneration = header.ConfigurationGeneration,
        Term = header.Term,
        LeaderNodeId = header.LeaderNodeId,
        SenderNodeId = header.SenderNodeId,
    };

    /// <summary>Maps the wire reply of an append to the outcome the leader works with.</summary>
    /// <param name="response">The wire reply.</param>
    /// <returns>The outcome, which carries the memory pressure flag of the follower; an older node never sets it.</returns>
    internal static FollowerLogAppendResult MapAppendResponse(AppendReplicaEntriesResponse response)
    {
        ArgumentNullException.ThrowIfNull(response);
        return new FollowerLogAppendResult(response.Success, response.RefusalCode, response.Term, response.LastLogIndex) { MemoryPressure = response.MemoryPressure };
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

    private static AppendReplicaEntriesRequest MapRequest(in ReplicaRpcHeader header, in FollowerBatch batch)
    {
        var request = new AppendReplicaEntriesRequest
        {
            Header = MapHeader(in header),
            PrevLogIndex = batch.PrevLogIndex,
            PrevLogTerm = batch.PrevLogTerm,
            LeaderCommitIndex = batch.LeaderCommitIndex,
        };
        var entries = batch.Entries;
        for (var i = 0; i < entries.Count; i++)
        {
            if (ReplicaLogCodec.Decode(entries[i].Payload) is not { } record)
                throw new InvalidOperationException("Replica log entry carries an undecodable canonical payload.");

            request.Entries.Add(MapEntry(in record));
        }

        return request;
    }

    private static ReplicaVoteRequest MapVoteRequest(in ReplicaRpcHeader header, ulong lastLogIndex, ulong lastLogTerm)
    {
        // A candidate claims no leadership: the voter ignores the leader identity, so none is sent and the caller's value,
        // which may be null, never reaches the protobuf setter.
        return new ReplicaVoteRequest
        {
            Header = new ReplicationEnvelopeHeader
            {
                SchemaVersion = EnvelopeSchema.Version,
                GroupId = header.GroupId,
                TopologyFingerprint = ByteString.CopyFrom(header.TopologyFingerprint.Span),
                ConfigurationGeneration = header.ConfigurationGeneration,
                Term = header.Term,
                LeaderNodeId = string.Empty,
                SenderNodeId = header.SenderNodeId,
            },
            LastLogIndex = lastLogIndex,
            LastLogTerm = lastLogTerm,
        };
    }
}
