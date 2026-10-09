using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Rocks;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Errors;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Grpc;

/// <summary>The entry node sends the parsed request to the key owner unchanged and relays the owner outcome as is.</summary>
public sealed class OwnerRpcForwarderTests : DisposableServerUnitTestBase
{
    private const string ClientId = "jwt:client";
    private const string Owner = "node-b";
    private const string OperationId = "0123456789abcdef0123456789abcdef";

    private readonly Meter _testMeter = new("test-owner-forwarder");

    /// <summary>Admission is taken on the cache transport with the real client identity, and a refusal never reaches the owner.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AdmissionPrecedesTheHop(CancellationToken cancellationToken)
    {
        var invoker = new CapturingCallInvoker(static _ => new SetAsyncResponse());
        var seen = new List<(string Transport, string Operation, string ClientId)>();
        var forwarder = CreateForwarder(invoker, CreatePolicy(), CreateGate(seen, Decision.Accepted()));

        _ = await forwarder.SetEntryAsync(Owner, new SetEntryAsyncRequest { CacheName = "c", Key = "k", OperationId = OperationId }, cancellationToken);

        _ = await Assert.That(seen.Count).IsEqualTo(1);
        _ = await Assert.That(seen[0]).IsEqualTo(("cache", "set", ClientId));

        var refusedInvoker = new CapturingCallInvoker(static _ => new RemoveAsyncResponse());
        var refusing = CreateForwarder(refusedInvoker, CreatePolicy(), CreateGate([], Decision.Rejected("busy")));
        var refusal = await NodeAsyncAssert.ThrowsAsync<SquirixException>(refusing.RemoveAsync(Owner, new RemoveAsyncRequest { CacheName = "c", Key = "k", OperationId = OperationId }, cancellationToken));

        _ = await Assert.That(refusal.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);
        _ = await Assert.That(refusedInvoker.Requests.Count).IsEqualTo(0);
    }

    /// <summary>A write that may have committed on the owner is passed through without a second attempt.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommitOutcomeUnknownIsNotRetried(CancellationToken cancellationToken)
    {
        var invoker = new CapturingCallInvoker(failure: static () => new RpcException(new Status(StatusCode.Unavailable, ServerOpContract.CommitOutcomeUnknownDetail)));
        var forwarder = CreateForwarder(invoker);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            forwarder.SetEntryAsync(Owner, new SetEntryAsyncRequest { CacheName = "c", Key = "k", OperationId = OperationId }, cancellationToken));

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(failure.Status.Detail).IsEqualTo(ServerOpContract.CommitOutcomeUnknownDetail);
        _ = await Assert.That(invoker.Requests.Count).IsEqualTo(1);
    }

    /// <summary>
    /// A hop that failed to connect, as a raw failure or as the client status that carries it, surfaces as the retryable unreachable failure
    /// raised on this node; a transport failure that may follow a sent request stays an ordinary Unavailable.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConnectionFailureMapsToUnavailable(CancellationToken cancellationToken)
    {
        var refused = CreateForwarder(new CapturingCallInvoker(failure: static () => new HttpRequestException(HttpRequestError.ConnectionError, "connection refused")));
        var started = CreateForwarder(new CapturingCallInvoker(
            failure: static () => new RpcException(new Status(StatusCode.Unavailable, "start failed", new HttpRequestException(HttpRequestError.ConnectionError, "refused")))));
        var reset = CreateForwarder(new CapturingCallInvoker(failure: static () => new IOException("connection reset")));
        var request = new GetValueAsyncRequest { CacheName = "c", Key = "k" };

        var refusedFailure = await NodeAsyncAssert.ThrowsAsync<RpcException>(refused.GetValueAsync(Owner, request, cancellationToken));
        var startedFailure = await NodeAsyncAssert.ThrowsAsync<RpcException>(started.GetValueAsync(Owner, request, cancellationToken));
        var resetFailure = await NodeAsyncAssert.ThrowsAsync<RpcException>(reset.GetValueAsync(Owner, request, cancellationToken));

        _ = await Assert.That(refusedFailure.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(refusedFailure.Status.Detail).IsEqualTo(ServerOpContract.OwnerUnreachableDetail);
        _ = await Assert.That(OwnerUnreachableFailure.IsLocal(refusedFailure) && OwnerUnreachableFailure.IsLocal(startedFailure)).IsTrue();
        _ = await Assert.That(resetFailure.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(resetFailure.Status.Detail).IsEqualTo("The connection to key owner 'node-b' failed after the call may have reached it.");
        _ = await Assert.That(OwnerUnreachableFailure.IsLocal(resetFailure)).IsFalse();
    }

    /// <summary>
    /// A failure the owner sent with the unreachable detail, or a client failure after the call started, is relayed and never taken for a
    /// failed connect of this node.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RelayedUnreachableIsNotLocal(CancellationToken cancellationToken)
    {
        var relayed = CreateForwarder(new CapturingCallInvoker(
            failure: static () => new RpcException(new Status(StatusCode.Unavailable, ServerOpContract.OwnerUnreachableDetail))));
        var broken = CreateForwarder(new CapturingCallInvoker(
            failure: static () => new RpcException(new Status(StatusCode.Unavailable, "start failed", new HttpRequestException(HttpRequestError.ResponseEnded, "ended")))));
        var request = new GetValueAsyncRequest { CacheName = "c", Key = "k" };

        var relayedFailure = await NodeAsyncAssert.ThrowsAsync<RpcException>(relayed.GetValueAsync(Owner, request, cancellationToken));
        var brokenFailure = await NodeAsyncAssert.ThrowsAsync<RpcException>(broken.GetValueAsync(Owner, request, cancellationToken));

        _ = await Assert.That(relayedFailure.Status.Detail).IsEqualTo(ServerOpContract.OwnerUnreachableDetail);
        _ = await Assert.That(OwnerUnreachableFailure.IsLocal(relayedFailure)).IsFalse();
        _ = await Assert.That(brokenFailure.Status.Detail).IsEqualTo("start failed");
        _ = await Assert.That(OwnerUnreachableFailure.IsLocal(brokenFailure)).IsFalse();
    }

    /// <summary>Every RPC reaches the owner as the same request instance on the matching method.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EveryRpcKeepsTheRequestInstance(CancellationToken cancellationToken)
    {
        var invoker = new CapturingCallInvoker(static method => CreateResponse(method));
        var forwarder = CreateForwarder(invoker);
        var entry = new CacheEntryWire { Expiration = Duration.FromTimeSpan(TimeSpan.FromMinutes(1)) };
        var getEntry = new GetEntryAsyncRequest { CacheName = "c", Key = "k" };
        var getExpiration = new GetExpirationAsyncRequest { CacheName = "c", Key = "k" };
        var getOrAdd = new GetOrAddAsyncRequest { CacheName = "c", Key = "k", Entry = entry, OperationId = OperationId };
        var getValue = new GetValueAsyncRequest { CacheName = "c", Key = "k" };
        var remove = new RemoveAsyncRequest { CacheName = "c", Key = "k", OperationId = OperationId };
        var removeExpiration = new RemoveExpirationAsyncRequest { CacheName = "c", Key = "k", OperationId = OperationId };
        var setEntry = new SetEntryAsyncRequest { CacheName = "c", Key = "k", Entry = entry, OperationId = OperationId };
        var touch = new TouchAsyncRequest { CacheName = "c", Key = "k", Expiration = entry.Expiration, OperationId = OperationId };
        var tryAdd = new TryAddEntryAsyncRequest { CacheName = "c", Key = "k", Entry = entry, OperationId = OperationId };
        var update = new UpdateAsyncRequest { CacheName = "c", Key = "k", Entry = entry, OperationId = OperationId };

        _ = await forwarder.GetEntryAsync(Owner, getEntry, cancellationToken);
        _ = await forwarder.GetExpirationAsync(Owner, getExpiration, cancellationToken);
        _ = await forwarder.GetOrAddAsync(Owner, getOrAdd, cancellationToken);
        _ = await forwarder.GetValueAsync(Owner, getValue, cancellationToken);
        _ = await forwarder.RemoveAsync(Owner, remove, cancellationToken);
        _ = await forwarder.RemoveExpirationAsync(Owner, removeExpiration, cancellationToken);
        _ = await forwarder.SetEntryAsync(Owner, setEntry, cancellationToken);
        _ = await forwarder.TouchAsync(Owner, touch, cancellationToken);
        _ = await forwarder.AddEntryIfAbsentAsync(Owner, tryAdd, cancellationToken);
        _ = await forwarder.UpdateAsync(Owner, update, cancellationToken);

        IMessage[] expected = [getEntry, getExpiration, getOrAdd, getValue, remove, removeExpiration, setEntry, touch, tryAdd, update];
        string[] methods = ["GetEntry", "GetExpiration", "GetOrAdd", "GetValue", "Remove", "RemoveExpiration", "SetEntry", "Touch", "TryAddEntry", "Update"];
        _ = await Assert.That(invoker.Requests.Count).IsEqualTo(expected.Length);
        for (var i = 0; i < expected.Length; i++)
        {
            _ = await Assert.That(invoker.Requests[i]).IsSameReferenceAs(expected[i]);
            _ = await Assert.That(invoker.Methods[i]).IsEqualTo(methods[i]);
        }
    }

    /// <summary>A pool that is disposed under the call, at the client lookup or in the policy, surfaces as Unavailable.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposedPoolMapsToUnavailable(CancellationToken cancellationToken)
    {
        var disposedPolicy = CreatePolicy();
        await disposedPolicy.DisposeAsync();
        var request = new GetValueAsyncRequest { CacheName = "c", Key = "k" };
        var policyDisposed = CreateForwarder(new CapturingCallInvoker(), disposedPolicy);
        var poolExpectations = new IServerClientPoolCreateExpectations();
        _ = poolExpectations.Setups.ForNode(Arg.Any<string>()).Callback(static _ => throw new ObjectDisposedException("pool"));
        var lookupDisposed = new OwnerRpcForwarder(poolExpectations.Instance(), CreateGate([], Decision.Accepted()), CreateClientIdResolver(), RingAgreements.Create());

        var fromPolicy = await NodeAsyncAssert.ThrowsAsync<RpcException>(policyDisposed.GetValueAsync(Owner, request, cancellationToken));
        var fromLookup = await NodeAsyncAssert.ThrowsAsync<RpcException>(lookupDisposed.GetValueAsync(Owner, request, cancellationToken));

        _ = await Assert.That(fromPolicy.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(fromPolicy.Status.Detail).IsEqualTo("ServerPeer client pool is disposed.");
        _ = await Assert.That(fromLookup.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(fromLookup.Status.Detail).IsEqualTo("ServerPeer client pool is disposed.");
    }

    /// <summary>The owner failure keeps its status, detail and application trailers, and loses the reserved protocol trailers.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RelayStripsReservedTrailers(CancellationToken cancellationToken)
    {
        var trailers = new Metadata
        {
            { "squirix-error-code", "stale-owner" },
            { "grpc-status-extra", "reserved" },
            { "detail-bin", [1, 2, 3] },
        };
        var invoker = new CapturingCallInvoker(failure: () => new RpcException(new Status(StatusCode.FailedPrecondition, "Key is owned by 'x'."), trailers));
        var forwarder = CreateForwarder(invoker);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            forwarder.RemoveAsync(Owner, new RemoveAsyncRequest { CacheName = "c", Key = "k", OperationId = OperationId }, cancellationToken));

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.FailedPrecondition);
        _ = await Assert.That(failure.Status.Detail).IsEqualTo("Key is owned by 'x'.");
        _ = await Assert.That(failure.Trailers.Count).IsEqualTo(2);
        _ = await Assert.That(failure.Trailers.GetValue("squirix-error-code")).IsEqualTo("stale-owner");
        _ = await Assert.That(Convert.ToHexString(failure.Trailers.GetValueBytes("detail-bin") ?? [])).IsEqualTo("010203");
    }

    /// <summary>The owner response is returned as is.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OwnerResponseIsReturnedUnchanged(CancellationToken cancellationToken)
    {
        var response = new GetOrAddAsyncResponse { Added = true, Found = true, Value = new CacheValue { StringValue = "v" } };
        var forwarder = CreateForwarder(new CapturingCallInvoker(_ => response));

        var result = await forwarder.GetOrAddAsync(Owner, new GetOrAddAsyncRequest { CacheName = "c", Key = "k", OperationId = OperationId }, cancellationToken);

        _ = await Assert.That(result).IsSameReferenceAs(response);
    }

    /// <summary>A ring-mismatch refusal from the owner fences the entry node and reaches the client with its trailer.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RingMismatchFromOwnerFencesEntryNode(CancellationToken cancellationToken)
    {
        var agreement = RingAgreements.Create();
        var invoker = new CapturingCallInvoker(failure: static () => RingMismatchFailure.Mismatch());
        var forwarder = CreateForwarder(invoker, agreement: agreement);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            forwarder.RemoveAsync(Owner, new RemoveAsyncRequest { CacheName = "c", Key = "k", OperationId = OperationId }, cancellationToken));

        _ = await Assert.That(RingMismatchFailure.IsMismatch(failure)).IsTrue();
        _ = await Assert.That(invoker.Requests.Count).IsEqualTo(1);
        _ = await Assert.That(agreement.IsFenced).IsTrue();
        _ = await Assert.That(agreement.FirstMismatch).IsEqualTo(new RingMismatchReport(Owner, RingMismatchDirection.Outbound, "unknown"));
    }

    /// <summary>A ring-fenced refusal from the owner does not fence the entry node, because the owner agrees with it on the ring.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RingFencedFromOwnerDoesNotFence(CancellationToken cancellationToken)
    {
        var agreement = RingAgreements.Create();
        var invoker = new CapturingCallInvoker(failure: static () => RingMismatchFailure.Fenced());
        var forwarder = CreateForwarder(invoker, agreement: agreement);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            forwarder.RemoveAsync(Owner, new RemoveAsyncRequest { CacheName = "c", Key = "k", OperationId = OperationId }, cancellationToken));

        _ = await Assert.That(RingMismatchFailure.IsRefusal(failure)).IsTrue();
        _ = await Assert.That(invoker.Requests.Count).IsEqualTo(1);
        _ = await Assert.That(agreement.IsFenced).IsFalse();
    }

    /// <summary>A stale-owner refusal from the owner does not fence the entry node.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaleOwnerFromOwnerDoesNotFence(CancellationToken cancellationToken)
    {
        var agreement = RingAgreements.Create();
        var invoker = new CapturingCallInvoker(failure: static () => StaleOwnerFailure.Create("node-c", Owner));
        var forwarder = CreateForwarder(invoker, agreement: agreement);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            forwarder.RemoveAsync(Owner, new RemoveAsyncRequest { CacheName = "c", Key = "k", OperationId = OperationId }, cancellationToken));

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.FailedPrecondition);
        _ = await Assert.That(agreement.IsFenced).IsFalse();
    }

    /// <summary>An admission refusal by the owner passes through to the client after one attempt.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ResourceExhaustedIsNotRetried(CancellationToken cancellationToken)
    {
        var invoker = new CapturingCallInvoker(failure: static () => new RpcException(new Status(StatusCode.ResourceExhausted, "Server is overloaded (busy).")));
        var forwarder = CreateForwarder(invoker);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            forwarder.TouchAsync(Owner, new TouchAsyncRequest { CacheName = "c", Key = "k", OperationId = OperationId }, cancellationToken));

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.ResourceExhausted);
        _ = await Assert.That(failure.Status.Detail).IsEqualTo("Server is overloaded (busy).");
        _ = await Assert.That(invoker.Requests.Count).IsEqualTo(1);
    }

    /// <summary>The forwarding defaults size the per-owner permits as half of the entry admission limit.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardingDefaultsSizeThePermits(CancellationToken cancellationToken)
    {
        _ = await Assert.That(ForwardingCallPolicyDefaults.MaxConcurrentPerPeer(256)).IsEqualTo(128);
        _ = await Assert.That(ForwardingCallPolicyDefaults.MaxConcurrentPerPeer(3)).IsEqualTo(1);
        _ = await Assert.That(ForwardingCallPolicyDefaults.MaxConcurrentPerPeer(1)).IsEqualTo(1);

        var instrumentation = new ServerCallPolicyInstrumentation(new ServerCallPolicyMetrics(_testMeter), new ServerRpcTimeoutMetrics(_testMeter));
        await using var policy = ForwardingCallPolicyDefaults.Create(instrumentation, Owner, 2, TimeProvider.System);
        var runs = new int[1];
        using var release = new SemaphoreSlim(0);
        var first = policy.ExecuteAsync(
            release,
            static async (gate, ct) =>
            {
                await gate.WaitAsync(ct);
                return 1;
            },
            cancellationToken);

        var second = policy.ExecuteAsync(
            runs,
            static (counter, _) =>
            {
                counter[0]++;
                return ValueTask.FromResult(2);
            },
            cancellationToken);
        var busy = await NodeAsyncAssert.ThrowsAsync<SquirixException, int>(second);
        _ = release.Release();

        _ = await Assert.That(busy.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);
        _ = await Assert.That(runs[0]).IsEqualTo(0);
        _ = await Assert.That(await first).IsEqualTo(1);
    }

    /// <summary>An Unavailable from the owner is relayed after one attempt and frees the entry slot.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnavailableFromOwnerIsNotRetried(CancellationToken cancellationToken)
    {
        var instrumentation = new ServerCallPolicyInstrumentation(new ServerCallPolicyMetrics(_testMeter), new ServerRpcTimeoutMetrics(_testMeter));
        await using var policy = ForwardingCallPolicyDefaults.Create(instrumentation, Owner, 8, TimeProvider.System);
        using var gate = new AdmissionGate(new AdmissionOptions { MaxInFlight = 8, MaxQueue = 1, SlowdownThreshold = 1, MaxSlowdownDelay = TimeSpan.Zero }, new BackpressureMetrics(_testMeter), TimeProvider.System);
        var invoker = new CapturingCallInvoker(failure: static () => new RpcException(new Status(StatusCode.Unavailable, "owner is down")));
        var forwarder = CreateForwarder(invoker, policy, gate);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            forwarder.TouchAsync(Owner, new TouchAsyncRequest { CacheName = "c", Key = "k", OperationId = OperationId }, cancellationToken));

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(invoker.Requests.Count).IsEqualTo(1);
        _ = await Assert.That(gate.InFlight).IsEqualTo(0);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static object CreateResponse(string method) => method switch
    {
        "GetEntry" => new GetEntryAsyncResponse(),
        "GetExpiration" => new GetExpirationAsyncResponse(),
        "GetOrAdd" => new GetOrAddAsyncResponse(),
        "GetValue" => new GetValueAsyncResponse(),
        "Remove" => new RemoveAsyncResponse(),
        "RemoveExpiration" => new RemoveExpirationAsyncResponse(),
        "SetEntry" => new SetAsyncResponse(),
        "Touch" => new TouchAsyncResponse(),
        "TryAddEntry" => new TryAddAsyncResponse(),
        "Update" => new UpdateAsyncResponse(),
        _ => throw new ArgumentOutOfRangeException(nameof(method), method, "Unsupported method."),
    };

    private static IBackpressureClientIdResolver CreateClientIdResolver()
    {
        var expectations = new IBackpressureClientIdResolverCreateExpectations();
        _ = expectations.Setups.Resolve().ReturnValue(ClientId);
        return expectations.Instance();
    }

    private static IBackpressureGate CreateGate(List<(string Transport, string Operation, string ClientId)> seen, Decision decision)
    {
        var expectations = new IBackpressureGateCreateExpectations();
        _ = expectations.Setups.AcquireAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                        .Callback((transport, operation, clientId, _) =>
                         {
                             seen.Add((transport, operation, clientId));
                             return ValueTask.FromResult((decision, Lease.Empty));
                         });
        return expectations.Instance();
    }

    private OwnerRpcForwarder CreateForwarder(CapturingCallInvoker invoker, IServerCallPolicy? policy = null, IBackpressureGate? gate = null, RingAgreement? agreement = null)
    {
        var poolExpectations = new IServerClientPoolCreateExpectations();
        var client = new SquirixCacheService.SquirixCacheServiceClient(invoker);
        var callPolicy = policy ?? CreatePolicy();
        _ = poolExpectations.Setups.ForNode(Arg.Any<string>()).ReturnValue(client);
        _ = poolExpectations.Setups.PolicyFor(Arg.Any<string>()).ReturnValue(callPolicy);
        return new OwnerRpcForwarder(poolExpectations.Instance(), gate ?? CreateGate([], Decision.Accepted()), CreateClientIdResolver(), agreement ?? RingAgreements.Create());
    }

    private ServerCallPolicy CreatePolicy() => new(
        new ServerCallPolicyInstrumentation(new ServerCallPolicyMetrics(_testMeter), new ServerRpcTimeoutMetrics(_testMeter)),
        3,
        64,
        Owner,
        TimeProvider.System,
        new CallPolicyTimeouts(TimeSpan.FromSeconds(1), TimeSpan.Zero, TimeSpan.Zero));
}
