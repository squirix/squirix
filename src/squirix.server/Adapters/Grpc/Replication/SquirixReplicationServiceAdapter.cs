using System;
using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography.X509Certificates;
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
    private readonly MtlsOptions _mtlsOptions;
    private readonly string _nodeId;
    private readonly VerifiedPeerCertificates? _peers;
    private readonly TopologyFingerprint _topologyFingerprint;
    private readonly ReplicaTransferReceiver? _transfers;
    private readonly bool _votesEnabled;

    /// <summary>Initializes a new instance of the <see cref="SquirixReplicationServiceAdapter" /> class.</summary>
    /// <param name="cluster">Cluster topology configuration.</param>
    /// <param name="mtlsOptions">Cluster mTLS options of this node.</param>
    /// <param name="peers">The verifier of the caller certificates of open connections; <see langword="null" /> when internode mTLS is disabled, which makes the internal listener unavailable.</param>
    /// <param name="groups">Replica group registry of this node; <see langword="null" /> keeps the adapter a refusing stub.</param>
    /// <param name="members">The replica sets of the served groups, which bound who may ask for a vote; required whenever <paramref name="groups" /> is given.</param>
    /// <param name="transfers">The receiver of state transfers of this node; <see langword="null" /> refuses every transfer message as not ready.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="cluster" /> or <paramref name="mtlsOptions" /> is <see langword="null" />, or
    /// <paramref name="groups" /> is given without <paramref name="members" />.
    /// </exception>
    internal SquirixReplicationServiceAdapter(
        TopologyOptions cluster,
        MtlsOptions mtlsOptions,
        VerifiedPeerCertificates? peers,
        ReplicaGroupRegistry? groups = null,
        IReplicaMembership? members = null,
        ReplicaTransferReceiver? transfers = null)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentNullException.ThrowIfNull(mtlsOptions);
        _mtlsOptions = mtlsOptions;
        _transfers = transfers;
        _peers = peers;
        _follower = groups == null ? null : new ReplicaFollower(groups, members ?? throw new ArgumentNullException(nameof(members)));

        // The static leader resumes at the durable term of its log, so a network vote must not raise that term: it would let the
        // static leader lead a new term no election granted. Votes stay disabled while automatic failover is off, and for the group
        // this node owns while no election driver runs for it, so its leader term never comes from an election.
        _votesEnabled = cluster.AutomaticFailoverEnabled;
        _nodeId = cluster.NodeId;

        _topologyFingerprint = TopologyFingerprint.CreateFromTopology(cluster, _mtlsOptions);
        _configurationGeneration = cluster.ConfigurationGeneration;
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
        _follower?.NotePeerContact(header.SenderNodeId);
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
            Role = _follower?.RoleOf(header.GroupId) ?? "follower",
            LastLogIndex = current.LastLogIndex,
            CommitIndex = current.CommitIndex,
            Readiness = ReplicaReadinessNames.Of(current.Readiness),
            TopologyFingerprint = ByteString.CopyFrom(current.TopologyFingerprint.ToArray()),
            ConfigurationGeneration = current.ConfigurationGeneration,
            RefusalCode = string.Empty,
            LastLogTerm = current.LastLogTerm,
            AppliedIndex = current.LastAppliedIndex,
            MemberState = GroupMemberStateNames.Of(current.MemberState),
        };
    }

    public override async Task<OfferReplicaBootstrapResponse> OfferReplicaBootstrap(OfferReplicaBootstrapRequest request, ServerCallContext context)
    {
        var h = EnsureHeader(request.Header, context, false);
        _follower?.NotePeerContact(h.SenderNodeId);
        var result = _follower == null
            ? new ReplicaBootstrapOfferResult(false, null, RefusalCodes.NotReady)
            : await _follower.AcceptOfferAsync(h.GroupId, h.SenderNodeId, h.TopologyFingerprint.ToByteArray(), h.ConfigurationGeneration, context.CancellationToken).ConfigureAwait(false);
        return new OfferReplicaBootstrapResponse
        {
            Accepted = result.Accepted,
            MemberState = result.MemberState is { } state ? GroupMemberStateNames.Of(state) : string.Empty,
            RefusalCode = result.RefusalCode,
        };
    }

    public override async Task<ReplicaVoteResponse> PreVote(ReplicaVoteRequest request, ServerCallContext context)
    {
        var h = EnsureHeader(request.Header, context, false);
        if (RefusesVotes(h.GroupId))
            return StubVoteRefusal();

        var result = await _follower.PreVoteAsync(h.GroupId, h.TopologyFingerprint.ToByteArray(), h.ConfigurationGeneration, Ballot(h, request), context.CancellationToken)
                                    .ConfigureAwait(false);
        return MapVote(in result);
    }

    public override async Task<ReplicaVoteResponse> RequestVote(ReplicaVoteRequest request, ServerCallContext context)
    {
        var header = EnsureHeader(request.Header, context, false);
        if (RefusesVotes(header.GroupId))
            return StubVoteRefusal();

        var result = await _follower.RequestVoteAsync(header.GroupId, header.TopologyFingerprint.ToByteArray(), header.ConfigurationGeneration, Ballot(header, request), context.CancellationToken)
                                    .ConfigureAwait(false);
        return MapVote(in result);
    }

    public override async Task<TransferReplicaStateResponse> TransferReplicaState(TransferReplicaStateRequest request, ServerCallContext context)
    {
        _ = EnsureHeader(request.Header, context, true);
        return _transfers == null
            ? new TransferReplicaStateResponse { Term = 0, Success = false, RefusalCode = RefusalCodes.NotReady }
            : await ReplicaTransferWire.ApplyAsync(_transfers, request, context.CancellationToken).ConfigureAwait(false);
    }

    /// <summary>Builds the ballot of a vote request; the candidate is the verified sender, never the claimed leader.</summary>
    /// <param name="header">Validated replication envelope identity.</param>
    /// <param name="request">The vote request.</param>
    /// <returns>The ballot for the follower.</returns>
    private static ElectionVoteRequest Ballot(ReplicationEnvelopeHeader header, ReplicaVoteRequest request) =>
        new(header.SenderNodeId, header.Term, request.LastLogIndex, request.LastLogTerm);

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
        entry.DecidedUtcTicks,
        entry.ResolvedUtcTicks,
        entry.PayloadChecksum);

    private static ReplicaVoteResponse MapVote(in FollowerLogVoteResult result) => new()
    {
        Term = result.CurrentTerm,
        Granted = result.Granted,
        RefusalCode = result.RefusalCode,
    };

    private static AppendReplicaEntriesResponse StubAppendRefusal(ReplicationEnvelopeHeader header) => new()
    {
        Term = header.Term,

        // When refusing to append entries, report the follower's last log index as a conflict hint. This stub follower has no log; return 0 rather than
        // echoing the leader's PrevLogIndex which would mislead the leader.
        LastLogIndex = 0,
        Success = false,
        RefusalCode = RefusalCodes.NotReady,
    };

    /// <summary>Refuses a vote without reaching storage: this node serves no groups, automatic failover is off, or this node leads the group statically.</summary>
    /// <returns>A refusal reporting term zero, which observes no term.</returns>
    private static ReplicaVoteResponse StubVoteRefusal() => new()
    {
        Term = 0,
        Granted = false,
        RefusalCode = RefusalCodes.NotReady,
    };

    /// <summary>Checks whether votes for a group are refused before storage is reached.</summary>
    /// <param name="groupId">Replica group identifier of the vote.</param>
    /// <returns>
    /// <see langword="true" /> when this node serves no groups, automatic failover is off, or the group is the one this node owns,
    /// whose identifier is this node's identifier, and no election driver runs for it, so this node leads it statically.
    /// </returns>
    [MemberNotNullWhen(false, nameof(_follower))]
    private bool RefusesVotes(string groupId) =>
        _follower == null || !_votesEnabled || (string.Equals(groupId, _nodeId, StringComparison.Ordinal) && !_follower.IsElectionDriven(groupId));

    private ReplicationEnvelopeHeader EnsureHeader(ReplicationEnvelopeHeader? header, ServerCallContext context, bool requireLeader)
    {
        if (header == null || string.IsNullOrWhiteSpace(header.SenderNodeId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Replication envelope header with sender_node_id is required."));

        if (requireLeader && string.IsNullOrWhiteSpace(header.LeaderNodeId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Replication envelope header with leader_node_id is required."));

        _ = PeerAuth.EnsureTrustedPeer(context, _mtlsOptions, _peers, header.SenderNodeId, requireLeader ? header.LeaderNodeId : null);

        const string m = "Unsupported replication envelope schema version.";
        return header.SchemaVersion != EnvelopeSchema.Version ? throw new RpcException(new Status(StatusCode.InvalidArgument, m)) : header;
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
        /// <param name="options">Cluster mTLS options.</param>
        /// <param name="peers">
        /// The certificates of open connections already verified against the cluster trust root and the configured peers; <see langword="null" />
        /// when mTLS is not configured.
        /// </param>
        /// <param name="claimedSenderNodeId">Sender node id claimed by the request envelope.</param>
        /// <param name="claimedLeaderNodeId">Leader node id claimed by the request envelope; null when the operation is not leader-authorized.</param>
        /// <returns>Validated peer node id from the client certificate.</returns>
        /// <exception cref="RpcException">Thrown when the call is not a trusted internal replication peer.</exception>
        internal static string EnsureTrustedPeer(
            ServerCallContext context,
            MtlsOptions options,
            VerifiedPeerCertificates? peers,
            string claimedSenderNodeId,
            string? claimedLeaderNodeId = null)
        {
            ArgumentNullException.ThrowIfNull(context);
            ArgumentNullException.ThrowIfNull(options);
            ArgumentException.ThrowIfNullOrWhiteSpace(claimedSenderNodeId);

            if (peers == null || options.InternalListenPort <= 0)
                throw new RpcException(new Status(StatusCode.Unavailable, "Internal replication listener is not configured."));

            var httpContext = context.GetHttpContext();
            if (!InternalListenerConnection.ArrivedOnInternalPort(httpContext, options.InternalListenPort))
                throw new RpcException(new Status(StatusCode.PermissionDenied, "Replication service is bound to the internal mTLS listener only."));

            var certificate = httpContext.Connection.ClientCertificate ?? ThrowHelper.Throw<X509Certificate2>(
                new RpcException(new Status(StatusCode.Unauthenticated, "Replication requires a trusted peer client certificate.")));

            // The chain is built on the first call of a connection only; later calls reuse its verdict while the certificate stays valid.
            if (!peers.TryVerify(certificate, out var certificateNodeId))
                throw new RpcException(new Status(StatusCode.Unauthenticated, "Replication peer certificate is not a configured cluster member."));

            var senderMatches = string.Equals(certificateNodeId, claimedSenderNodeId, StringComparison.Ordinal);
            var leaderMatches = claimedLeaderNodeId == null || string.Equals(certificateNodeId, claimedLeaderNodeId, StringComparison.Ordinal);
            return (senderMatches, leaderMatches) switch
            {
                (false, _) => throw new RpcException(new Status(StatusCode.Unauthenticated, "Replication sender_node_id does not match the peer certificate NodeId.")),
                (true, false) => throw new RpcException(new Status(StatusCode.PermissionDenied, "Replication leader_node_id does not match the peer certificate NodeId.")),
                (true, true) => certificateNodeId,
            };
        }
    }
}
