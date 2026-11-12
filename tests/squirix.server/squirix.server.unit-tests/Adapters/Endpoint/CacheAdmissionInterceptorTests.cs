using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Endpoint;

/// <summary>Admission covers duplicate RPCs and is shared only with the same host's nested pipeline.</summary>
public sealed class CacheAdmissionInterceptorTests : ServerUnitTestBase
{
    /// <summary>Completed outcomes obey capacity and rate limits, without charging fresh mutations twice.</summary>
    /// <param name="rateLimit">Whether to exhaust rate tokens instead of the concurrency slot.</param>
    /// <param name="cancellationToken">Test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ReplayRequiresAdmissionTest(bool rateLimit, CancellationToken cancellationToken)
    {
        using var meter = new Meter("replay-admission-test");
        var clock = new FakeTimeProvider();
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 1,
                MaxQueue = 0,
                SlowdownThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
                NodeRateLimitPerSecond = rateLimit ? 1 : null,
                NodeRateLimitBurst = rateLimit ? 1 : null,
            },
            new BackpressureMetrics(meter),
            clock);
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(meter), clock);
        var coordinator = new RpcMutationIdempotencyCoordinator(store, NullLogger<RpcMutationIdempotencyCoordinator>.Instance);
        var interceptor = new CacheAdmissionInterceptor(gate, SharedClientIdResolver.Instance);
        var context = new TestServerCallContext(method: "/cache/TryAddEntry");
        IBackpressureGate pipelineGate = gate;
        Task<TryAddAsyncResponse> HandleAsync(string request, ServerCallContext call)
        {
            return coordinator.ExecuteAsync(
                request,
                static _ => "fingerprint",
                pipelineGate,
                static async (inner, token) =>
                {
                    var (decision, lease) = await inner.AcquireAsync("cache", "try_add", "client", token);
                    using (lease)
                        return new TryAddAsyncResponse { Added = decision.IsAccepted };
                },
                call.CancellationToken);
        }

        var first = await interceptor.UnaryServerHandler("0123456789abcdef0123456789abcdef", context, HandleAsync);
        _ = await Assert.That(first.Added).IsTrue();
        var held = rateLimit ? Lease.Empty : (await gate.AcquireAsync("cache", "set", "client", cancellationToken)).Lease;
        using (held)
        {
            var error = await NodeAsyncAssert.ThrowsAsync<RpcException>(
                interceptor.UnaryServerHandler("0123456789abcdef0123456789abcdef", context, HandleAsync),
                TimeSpan.FromSeconds(10),
                TimeProvider.System,
                cancellationToken);
            _ = await Assert.That(error.StatusCode).IsEqualTo(StatusCode.ResourceExhausted);
        }

        clock.Advance(TimeSpan.FromSeconds(1));
        var replay = await interceptor.UnaryServerHandler("0123456789abcdef0123456789abcdef", context, HandleAsync);
        _ = await Assert.That(replay.Added).IsTrue();
        _ = await Assert.That(gate.InFlight).IsEqualTo(0);
    }

    /// <summary>An in-flight duplicate is refused before joining the original execution when the node has no capacity.</summary>
    /// <param name="cancellationToken">Test cancellation token.</param>
    [Test]
    public async Task PendingDuplicateCannotBypassCapacityTest(CancellationToken cancellationToken)
    {
        using var meter = new Meter("admission-test");
        using var gate = CreateGate(meter);
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(meter), TimeProvider.System);
        var coordinator = new RpcMutationIdempotencyCoordinator(store, NullLogger<RpcMutationIdempotencyCoordinator>.Instance);
        var interceptor = new CacheAdmissionInterceptor(gate, SharedClientIdResolver.Instance);
        var release = new TaskCompletionSource<TryAddAsyncResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        var context = new TestServerCallContext(method: "/squirix.cache.v2.SquirixCacheService/TryAddEntry");
        Task<TryAddAsyncResponse> HandleAsync(string request, ServerCallContext call)
        {
            return coordinator.ExecuteAsync(request, static _ => "fingerprint", release, static (state, _) => state.Task, call.CancellationToken);
        }

        var first = interceptor.UnaryServerHandler("0123456789abcdef0123456789abcdef", context, HandleAsync);
        try
        {
            var error = await NodeAsyncAssert.ThrowsAsync<RpcException>(
                interceptor.UnaryServerHandler("0123456789abcdef0123456789abcdef", context, HandleAsync),
                TimeSpan.FromSeconds(10),
                TimeProvider.System,
                cancellationToken);
            _ = await Assert.That(error.StatusCode).IsEqualTo(StatusCode.ResourceExhausted);
            _ = await Assert.That(gate.InFlight).IsEqualTo(1);
            _ = await Assert.That(gate.QueueDepth).IsEqualTo(0);
        }
        finally
        {
            _ = release.TrySetResult(new TryAddAsyncResponse { Added = true });
            _ = await first;
        }

        _ = await Assert.That(gate.InFlight).IsEqualTo(0);
    }

    /// <summary>Nested admission neither consumes another slot nor grants admission on a different host.</summary>
    /// <param name="cancellationToken">Test cancellation token.</param>
    [Test]
    public async Task NestedAdmissionIsHostScopedTest(CancellationToken cancellationToken)
    {
        using var meter = new Meter("admission-test");
        using var otherMeter = new Meter("other-admission-test");
        using var gate = CreateGate(meter);
        using var other = CreateGate(otherMeter);
        var (_, held) = await other.AcquireAsync("cache", "set", "client", cancellationToken);
        var (_, admittedLease) = await gate.AcquireAsync("cache", "set", "client", cancellationToken);
        using (held)
        using (admittedLease)
        using (InboundAdmissionScope.Enter(gate))
        {
            var (nested, nestedLease) = await gate.AcquireAsync("cache", "set", "client", cancellationToken);
            using (nestedLease)
            {
                _ = await Assert.That(nested.IsAccepted).IsTrue();
                _ = await Assert.That(gate.InFlight).IsEqualTo(1);
            }

            var (refused, refusedLease) = await other.AcquireAsync("cache", "set", "client", cancellationToken);
            refusedLease.Dispose();
            _ = await Assert.That(refused.IsAccepted).IsFalse();
        }
    }

    /// <summary>An execution context captured during admission cannot reuse the admission after the RPC ends.</summary>
    /// <param name="cancellationToken">Test cancellation token.</param>
    [Test]
    public async Task DisposedScopeInvalidatesContextTest(CancellationToken cancellationToken)
    {
        using var meter = new Meter("admission-test");
        using var gate = CreateGate(meter);
        var resume = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task<bool> inherited;
        using (InboundAdmissionScope.Enter(gate))
            inherited = ReadAfterResumeAsync(resume.Task, gate, cancellationToken);

        resume.SetResult();
        _ = await Assert.That(await inherited).IsFalse();
    }

    /// <summary>A failing handler releases its admission and restores the caller's context.</summary>
    /// <param name="cancellationToken">Test cancellation token.</param>
    [Test]
    public async Task HandlerFailureReleasesAdmissionTest(CancellationToken cancellationToken)
    {
        using var meter = new Meter("admission-test");
        using var gate = CreateGate(meter);
        var interceptor = new CacheAdmissionInterceptor(gate, SharedClientIdResolver.Instance);
        _ = await NodeAsyncAssert.ThrowsAsync<OperationCanceledException>(
            interceptor.UnaryServerHandler(
                "request",
                new TestServerCallContext(method: "/cache/SetEntry"),
                static (_, _) => Task.FromException<string>(new OperationCanceledException())),
            TimeSpan.FromSeconds(10),
            TimeProvider.System,
            cancellationToken);
        _ = await Assert.That(gate.InFlight).IsEqualTo(0);
        _ = await Assert.That(InboundAdmissionScope.IsAdmitted(gate)).IsFalse();
    }

    private static AdmissionGate CreateGate(Meter meter) => new(
        new AdmissionOptions { MaxInFlight = 1, MaxQueue = 0, SlowdownThreshold = 1, MaxSlowdownDelay = TimeSpan.Zero },
        new BackpressureMetrics(meter),
        TimeProvider.System);

    private static async Task<bool> ReadAfterResumeAsync(Task resume, IBackpressureGate gate, CancellationToken cancellationToken)
    {
        await resume.WaitAsync(cancellationToken);
        return InboundAdmissionScope.IsAdmitted(gate);
    }
}
