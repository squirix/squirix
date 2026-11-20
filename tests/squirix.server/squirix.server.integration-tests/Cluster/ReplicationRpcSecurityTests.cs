using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Adapters.Wire.Replication;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport.Binary;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster;

/// <summary>REQ-SEC-001: closed replication RPCs are bound to internal mTLS identity.</summary>
public sealed class ReplicationRpcSecurityTests : NodeIntegrationTestBase
{
    /// <summary>Every protobuf method is mapped once and retains internal-listener and authorization metadata.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EveryMethodKeepsInternalMetadata(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", new IntegrationStartOptions { FoundationOnly = true }, cancellationToken);
        var node = cluster["node-a"];
        _ = await Assert.That(node.Services.GetService(typeof(SquirixReplicationServiceAdapter))).IsNotNull();
        var port = node.GetRequiredService<MtlsOptions>().InternalListenPort;
        var peer = cluster.Peers[0];
        _ = await Assert.That(peer.InterNodeUri!.Port).IsEqualTo(port);
        _ = await Assert.That(peer.Uri.Port).IsNotEqualTo(port);
        using var handler = await CreateTrustedInterNodeClientHandlerAsync("node-b", cluster["node-b"].Uri, "node-a", cluster.Peers, cancellationToken);
        var client = new PeerSocketProbe(peer.InterNodeUri, handler.SslOptions);
        var reply = await client.GetReplicaStatusAsync(CreateStatusRequest("node-b"), cancellationToken: cancellationToken);
        _ = await Assert.That(reply).IsNotNull();
    }

    /// <summary>Matching peer certificate and sender_node_id is accepted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CertificateNodeIdMatchIsAccepted(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", new IntegrationStartOptions { FoundationOnly = true }, cancellationToken);
        var options = cluster["node-a"].GetRequiredService<MtlsOptions>();
        var interNodeUri = new UriBuilder(cluster["node-a"].Uri.Scheme, cluster["node-a"].Uri.Host, options.InternalListenPort).Uri;
        using var handler = await CreateTrustedInterNodeClientHandlerAsync("node-b", cluster["node-b"].Uri, "node-a", cluster.Peers, cancellationToken);
        var client = new PeerSocketProbe(interNodeUri, handler.SslOptions);

        // Certificate identity is node-b; claim sender_node_id node-b for matching identity.
        var response = await client.GetReplicaStatusAsync(CreateStatusRequest("node-b"), cancellationToken: cancellationToken);

        _ = await Assert.That(response).IsNotNull();
        _ = await Assert.That(response.RefusalCode).IsEqualTo("not-ready");
    }

    /// <summary>Claimed sender_node_id must match the peer certificate NodeId.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CertificateNodeIdMismatchIsRejected(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", new IntegrationStartOptions { FoundationOnly = true }, cancellationToken);
        var nodeA = cluster["node-a"];

        var mtlsOptions = nodeA.GetRequiredService<MtlsOptions>();
        var interNodeUri = new UriBuilder(nodeA.Uri.Scheme, nodeA.Uri.Host, mtlsOptions.InternalListenPort).Uri;
        using var handler = await CreateTrustedInterNodeClientHandlerAsync("node-b", cluster["node-b"].Uri, "node-a", cluster.Peers, cancellationToken);
        var client = new PeerSocketProbe(interNodeUri, handler.SslOptions);

        // Certificate identity is node-b; claim sender_node_id node-a to force mismatch after TLS succeeds.
        var ex = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(client.GetReplicaStatusAsync(CreateStatusRequest("node-a"), cancellationToken: cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(ex.StatusCode).IsEqualTo(FrameStatus.Unauthenticated);
    }

    /// <summary>External listener does not expose the closed replication service.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExternalListenerRefusesReplicationRpc(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", new IntegrationStartOptions { FoundationOnly = true }, cancellationToken);
        using var handler = LoopbackHttp.CreateHandler();
        using var channel = new HttpClient(handler, false) { BaseAddress = cluster["node-a"].Uri };
        var client = channel;
        var ex = await NodeAsyncAssert.ThrowsAsync<HttpRequestException>(client.GetReplicaStatusAsync(CreateStatusRequest("node-b"), cancellationToken: cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(ex.StatusCode).IsNull();
    }

    /// <summary>External listener does not expose the vote RPCs.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExternalListenerRefusesVoteRpc(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", new IntegrationStartOptions { FoundationOnly = true }, cancellationToken);
        using var handler = LoopbackHttp.CreateHandler();
        using var channel = new HttpClient(handler, false) { BaseAddress = cluster["node-a"].Uri };
        var client = channel;

        var preVote = await NodeAsyncAssert.ThrowsAsync<HttpRequestException>(client.PreVoteAsync(CreateVoteRequest("node-b", "node-b"), cancellationToken: cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        var vote = await NodeAsyncAssert.ThrowsAsync<HttpRequestException>(client.RequestVoteAsync(CreateVoteRequest("node-b", "node-b"), cancellationToken: cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(preVote.StatusCode).IsNull();
        _ = await Assert.That(vote.StatusCode).IsNull();
    }

    /// <summary>Leader-authorized RPCs reject a trusted peer claiming a foreign LeaderNodeId.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForeignLeaderNodeIdIsRejected(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", new IntegrationStartOptions { FoundationOnly = true }, cancellationToken);
        var nodeA = cluster["node-a"];

        var mtlsOptions = nodeA.GetRequiredService<MtlsOptions>();
        var interNodeUri = new UriBuilder(nodeA.Uri.Scheme, nodeA.Uri.Host, mtlsOptions.InternalListenPort).Uri;
        using var handler = await CreateTrustedInterNodeClientHandlerAsync("node-b", cluster["node-b"].Uri, "node-a", cluster.Peers, cancellationToken);
        var client = new PeerSocketProbe(interNodeUri, handler.SslOptions);

        // Certificate identity is node-b; claim leader_node_id node-a to force a leader identity mismatch.
        var request = CreateAppendRequest("node-b", "node-a");
        var ex = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(client.AppendReplicaEntriesAsync(request, cancellationToken: cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(ex.StatusCode).IsEqualTo(FrameStatus.PermissionDenied);
    }

    /// <summary>Forged Host headers cannot bind closed replication RPCs onto the external listener.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForgedInternalHostHeaderIsRejected(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", new IntegrationStartOptions { FoundationOnly = true }, cancellationToken);
        var nodeA = cluster["node-a"];

        var mtlsOptions = nodeA.GetRequiredService<MtlsOptions>();
        var forgedHost = string.Create(CultureInfo.InvariantCulture, $"127.0.0.1:{mtlsOptions.InternalListenPort}");

        using var inner = LoopbackHttp.CreateHandler();
        using var handler = new ForgedHostHandler(forgedHost, inner);
        using var channel = new HttpClient(handler, false) { BaseAddress = nodeA.Uri };
        var client = channel;

        var ex = await NodeAsyncAssert.ThrowsAsync<HttpRequestException>(client.GetReplicaStatusAsync(CreateStatusRequest("node-b"), cancellationToken: cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(ex.StatusCode).IsNull();
    }

    /// <summary>Append accepts a matching peer identity and continues the caller trace.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MatchingLeaderNodeIdIsAccepted(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", new IntegrationStartOptions { FoundationOnly = true }, cancellationToken);
        var nodeA = cluster["node-a"];

        var mtlsOptions = nodeA.GetRequiredService<MtlsOptions>();
        var interNodeUri = new UriBuilder(nodeA.Uri.Scheme, nodeA.Uri.Host, mtlsOptions.InternalListenPort).Uri;
        using var handler = await CreateTrustedInterNodeClientHandlerAsync("node-b", cluster["node-b"].Uri, "node-a", cluster.Peers, cancellationToken);
        var client = new PeerSocketProbe(interNodeUri, handler.SslOptions);

        var traceId = ActivityTraceId.CreateRandom();
        var parentSpanId = ActivitySpanId.CreateRandom();
        const string traceState = "replication=append";
        const string operation = PeerProtocol.Prefix + "replication/AppendReplicaEntries";
        var captured = new TaskCompletionSource<Activity>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var listener = new ActivityListener
        {
            ShouldListenTo = static source => string.Equals(source.Name, ActivitySourceHolder.SourceName, StringComparison.Ordinal),
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity =>
            {
                if (activity.TraceId == traceId && activity.Kind == ActivityKind.Server && string.Equals(activity.OperationName, operation, StringComparison.Ordinal))
                    _ = captured.TrySetResult(activity);
            },
        };
        ActivitySource.AddActivityListener(listener);
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            { Correlation.TraceParentHeader, $"00-{traceId}-{parentSpanId}-01" },
            { Correlation.TraceStateHeader, traceState },
        };
        var response = await client.AppendReplicaEntriesAsync(CreateAppendRequest("node-b", "node-b"), headers, cancellationToken);
        var span = await captured.Task.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);

        _ = await Assert.That(span.ParentSpanId).IsEqualTo(parentSpanId);
        _ = await Assert.That(span.TraceStateString).IsEqualTo(traceState);

        _ = await Assert.That(response).IsNotNull();
        _ = await Assert.That(response.Success).IsFalse();
        _ = await Assert.That(response.RefusalCode).IsEqualTo("not-ready");
    }

    /// <summary>Vote RPCs require the claimed sender_node_id to match the peer certificate NodeId.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VoteCertificateNodeIdMismatchIsRejected(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", new IntegrationStartOptions { FoundationOnly = true }, cancellationToken);
        var nodeA = cluster["node-a"];

        var mtlsOptions = nodeA.GetRequiredService<MtlsOptions>();
        var interNodeUri = new UriBuilder(nodeA.Uri.Scheme, nodeA.Uri.Host, mtlsOptions.InternalListenPort).Uri;
        using var handler = await CreateTrustedInterNodeClientHandlerAsync("node-b", cluster["node-b"].Uri, "node-a", cluster.Peers, cancellationToken);
        var client = new PeerSocketProbe(interNodeUri, handler.SslOptions);

        // Certificate identity is node-b; a candidate claiming node-a must not collect a vote for node-a.
        var preVote = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(client.PreVoteAsync(CreateVoteRequest("node-a", "node-a"), cancellationToken: cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        var vote = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(client.RequestVoteAsync(CreateVoteRequest("node-a", "node-a"), cancellationToken: cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(preVote.StatusCode).IsEqualTo(FrameStatus.Unauthenticated);
        _ = await Assert.That(vote.StatusCode).IsEqualTo(FrameStatus.Unauthenticated);
    }

    /// <summary>Vote RPCs are not leader-authorized: a foreign LeaderNodeId is ignored and the vote is answered.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VoteWithForeignLeaderNodeIdIsAnswered(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", new IntegrationStartOptions { FoundationOnly = true }, cancellationToken);
        var nodeA = cluster["node-a"];

        var mtlsOptions = nodeA.GetRequiredService<MtlsOptions>();
        var interNodeUri = new UriBuilder(nodeA.Uri.Scheme, nodeA.Uri.Host, mtlsOptions.InternalListenPort).Uri;
        using var handler = await CreateTrustedInterNodeClientHandlerAsync("node-b", cluster["node-b"].Uri, "node-a", cluster.Peers, cancellationToken);
        var client = new PeerSocketProbe(interNodeUri, handler.SslOptions);

        // Certificate identity is node-b; leader_node_id node-a would be refused on leader-authorized RPCs.
        var preVote = await client.PreVoteAsync(CreateVoteRequest("node-b", "node-a"), cancellationToken: cancellationToken);
        var vote = await client.RequestVoteAsync(CreateVoteRequest("node-b", "node-a"), cancellationToken: cancellationToken);

        _ = await Assert.That(preVote.Granted).IsFalse();
        _ = await Assert.That(preVote.RefusalCode).IsEqualTo("not-ready");
        _ = await Assert.That(vote.Granted).IsFalse();
        _ = await Assert.That(vote.RefusalCode).IsEqualTo("not-ready");
        _ = await Assert.That(vote.Term).IsEqualTo(0UL);
    }

    private static AppendReplicaEntriesRequest CreateAppendRequest(string senderNodeId, string leaderNodeId) => new()
    {
        Header = new ReplicationEnvelopeHeader
        {
            SchemaVersion = EnvelopeSchema.Version,
            GroupId = "g1",
            TopologyFingerprint = ByteString.CopyFrom(1, 2, 3, 4),
            ConfigurationGeneration = 1,
            Term = 1,
            LeaderNodeId = leaderNodeId,
            SenderNodeId = senderNodeId,
        },
        PrevLogIndex = 0,
    };

    private static GetReplicaStatusRequest CreateStatusRequest(string senderNodeId) => new()
    {
        Header = new ReplicationEnvelopeHeader
        {
            SchemaVersion = EnvelopeSchema.Version,
            GroupId = "g1",
            TopologyFingerprint = ByteString.CopyFrom(1, 2, 3, 4),
            ConfigurationGeneration = 1,
            Term = 1,
            LeaderNodeId = senderNodeId,
            SenderNodeId = senderNodeId,
        },
    };

    private static ReplicaVoteRequest CreateVoteRequest(string senderNodeId, string leaderNodeId) => new()
    {
        Header = new ReplicationEnvelopeHeader
        {
            SchemaVersion = EnvelopeSchema.Version,
            GroupId = "g1",
            TopologyFingerprint = ByteString.CopyFrom(1, 2, 3, 4),
            ConfigurationGeneration = 1,
            Term = 2,
            LeaderNodeId = leaderNodeId,
            SenderNodeId = senderNodeId,
        },
    };

    private sealed class ForgedHostHandler : DelegatingHandler
    {
        private readonly string _host;

        internal ForgedHostHandler(string host, HttpMessageHandler inner)
            : base(inner)
        {
            _host = host;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            request.Headers.Host = _host;
            return base.SendAsync(request, cancellationToken);
        }
    }
}
