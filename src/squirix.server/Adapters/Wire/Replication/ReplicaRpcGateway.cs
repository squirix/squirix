using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Adapters.Wire.Replication;

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
        var response = await lease.Client.SendAsync(PeerOperation.AppendReplicaEntries, MapRequest(in header, in batch), AppendReplicaEntriesResponse.Parser, lease.Token).ConfigureAwait(false);
        return MapAppendResponse(response);
    }

    /// <inheritdoc />
    public async Task SendTransferAsync(string target, ReplicaRpcHeader header, CancellationToken cancellationToken)
    {
        using var lease = _pool.LeaseChannel(target, cancellationToken);
        var request = new LeadershipTransferRequest { Header = MapHeader(in header), Target = target };
        _ = await lease.Client.SendAsync(PeerOperation.TransferLeadership, request, ReplicaVoteResponse.Parser, lease.Token).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task<FollowerLogVoteResult> PreVoteAsync(string nodeId, ReplicaRpcHeader header, ulong lastLogIndex, ulong lastLogTerm, CancellationToken cancellationToken) =>
        SendBallotAsync(true, nodeId, header, (lastLogIndex, lastLogTerm), null, cancellationToken);

    /// <inheritdoc />
    public Task<FollowerLogVoteResult> RequestVoteAsync(string nodeId, ReplicaRpcHeader header, ulong lastLogIndex, ulong lastLogTerm, CancellationToken cancellationToken) =>
        SendBallotAsync(false, nodeId, header, (lastLogIndex, lastLogTerm), null, cancellationToken);

    /// <inheritdoc />
    public Task<FollowerLogVoteResult> AskVoteAsync(
        bool preVote,
        string nodeId,
        ReplicaRpcHeader header,
        (ulong Index, ulong Term) last,
        (TimeSpan Timeout, TimeProvider Clock) replyBudget,
        CancellationToken cancellationToken) =>
        SendBallotAsync(preVote, nodeId, header, last, replyBudget, cancellationToken);

    /// <summary>Maps the replication envelope identity to its wire message.</summary>
    /// <param name="header">The envelope identity.</param>
    /// <returns>The wire header.</returns>
    internal static EnvelopeHeader MapHeader(in ReplicaRpcHeader header) => new()
    {
        SchemaVersion = EnvelopeSchema.Version,
        GroupId = header.GroupId,
        TopologyFingerprint = header.TopologyFingerprint,
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

    /// <summary>Maps an append batch to its wire request; the entry payloads are wrapped, not copied.</summary>
    /// <param name="header">The replication header.</param>
    /// <param name="batch">The batch to send.</param>
    /// <returns>The wire request.</returns>
    internal static AppendReplicaEntriesRequest MapRequest(in ReplicaRpcHeader header, in FollowerBatch batch) => new()
    {
        Header = MapHeader(in header),
        PrevLogIndex = batch.PrevLogIndex,
        PrevLogTerm = batch.PrevLogTerm,
        LeaderCommitIndex = batch.LeaderCommitIndex,
        Entries = batch.Entries,
    };

    private static ReplicaVoteRequest MapVoteRequest(in ReplicaRpcHeader header, ulong lastLogIndex, ulong lastLogTerm)
    {
        // A candidate makes no claim to leadership in a vote request.
        return new ReplicaVoteRequest
        {
            Header = new EnvelopeHeader
            {
                SchemaVersion = EnvelopeSchema.Version,
                GroupId = header.GroupId,
                TopologyFingerprint = header.TopologyFingerprint,
                ConfigurationGeneration = header.ConfigurationGeneration,
                Term = header.Term,
                LeaderNodeId = string.Empty,
                SenderNodeId = header.SenderNodeId,
            },
            LastLogIndex = lastLogIndex,
            LastLogTerm = lastLogTerm,
        };
    }

    private async Task<FollowerLogVoteResult> SendBallotAsync(
        bool preVote,
        string nodeId,
        ReplicaRpcHeader header,
        (ulong Index, ulong Term) last,
        (TimeSpan Timeout, TimeProvider Clock)? replyBudget,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        using var lease = _pool.LeaseChannel(nodeId, cancellationToken);
        var request = MapVoteRequest(in header, last.Index, last.Term);
        var operation = PeerOperation.RequestVote;
        if (preVote)
        {
            operation = PeerOperation.PreVote;
            if (header.TransferSource is { } source)
            {
                request.Header!.LeaderNodeId = source;
                operation = PeerOperation.TransferPreVote;
            }
        }

        var response = replyBudget is { } budget
            ? await lease.Client.SendElectionAsync(operation, request, ReplicaVoteResponse.Parser, budget, lease.Token).ConfigureAwait(false)
            : await lease.Client.SendAsync(operation, request, ReplicaVoteResponse.Parser, lease.Token).ConfigureAwait(false);
        return new FollowerLogVoteResult(response.Granted, response.RefusalCode, response.Term);
    }
}
