using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Rocks;
using Squirix.Server.Core;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.App.Decorators;

/// <summary>Verifies admission shaping runs outside the pipeline deadline.</summary>
public sealed class PipelineDeadlineOrderTests : ServerUnitTestBase
{
    /// <summary>Admission observes the caller token, while execution underneath still runs under the pipeline deadline.</summary>
    [Test]
    public async Task AdmissionBypassesPipelineDeadline()
    {
        var gateToken = new StrongBox<CancellationToken>();
        var innerToken = new StrongBox<CancellationToken>();
        var pipeline = CreatePipeline(CreateRecordingGate(gateToken), CreateRecordingInner(innerToken, false), TimeSpan.FromSeconds(10));

        _ = await pipeline.GetValueAsync("c", "k", CancellationToken.None);

        _ = await Assert.That(gateToken.Value.CanBeCanceled).IsFalse();
        _ = await Assert.That(innerToken.Value.CanBeCanceled).IsTrue();
    }

    /// <summary>A hung execution still faults with TimeoutException once the pipeline deadline expires.</summary>
    [Test]
    public async Task SlowExecutionStillHitsPipelineDeadline()
    {
        var pipeline = CreatePipeline(
            CreateRecordingGate(new StrongBox<CancellationToken>()),
            CreateRecordingInner(new StrongBox<CancellationToken>(), true),
            TimeSpan.FromMilliseconds(100));

        _ = await NodeAsyncAssert.ThrowsAsync<TimeoutException, NodeCacheValueResult<string>>(pipeline.GetValueAsync("c", "k", CancellationToken.None));
    }

    private static BackpressureCacheDecorator<string> CreatePipeline(IBackpressureGate gate, ILogicalNamespacedCache<string> inner, TimeSpan budget)
    {
        var deadline = new DeadlineCacheDecorator<string>(inner, Options.Create(new CachePipelineDeadlineOptions { DefaultOperationTimeout = budget }));
        var resolverExpectations = new IBackpressureClientIdResolverCreateExpectations();
        _ = resolverExpectations.Setups.Resolve().ReturnValue("test");
        return new BackpressureCacheDecorator<string>(deadline, gate, resolverExpectations.Instance());
    }

    /// <summary>Mocks a gate that accepts every operation and records the token it was acquired with.</summary>
    /// <param name="observedToken">Receives the token of the latest acquire.</param>
    /// <returns>The mocked gate.</returns>
    private static IBackpressureGate CreateRecordingGate(StrongBox<CancellationToken> observedToken)
    {
        var expectations = new IBackpressureGateCreateExpectations();
        _ = expectations.Setups.AcquireAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                        .Callback((_, _, _, cancellationToken) =>
                         {
                             observedToken.Value = cancellationToken;
                             return ValueTask.FromResult((Decision.Accepted(), Lease.Empty));
                         });
        return expectations.Instance();
    }

    /// <summary>Mocks the inner cache: value reads record their token and, when <paramref name="hang" /> is set, wait until it is canceled.</summary>
    /// <param name="observedToken">Receives the token of the latest value read.</param>
    /// <param name="hang">Whether value reads never complete on their own.</param>
    /// <returns>The mocked inner cache.</returns>
    private static ILogicalNamespacedCache<string> CreateRecordingInner(StrongBox<CancellationToken> observedToken, bool hang)
    {
        var expectations = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = expectations.Setups.GetValueAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                        .Callback(async (_, _, cancellationToken) =>
                         {
                             observedToken.Value = cancellationToken;
                             if (hang)
                                 await Task.Delay(Timeout.InfiniteTimeSpan, TimeProvider.System, cancellationToken).ConfigureAwait(false);

                             return new NodeCacheValueResult<string>(false, null);
                         });
        return expectations.Instance();
    }
}
