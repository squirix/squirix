using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Cluster;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Unit tests for how <see cref="ServerCallPolicy" /> treats ring refusals.</summary>
public sealed class RingRefusalCallPolicyTests : DisposableServerUnitTestBase
{
    private readonly Meter _testMeter = new("test-ring-refusal-policy");

    /// <summary>Ensures a ring refusal is attempted exactly once: a retry cannot fix a configuration disagreement.</summary>
    /// <param name="mismatch">Whether the refusal is the ring-mismatch failure rather than the ring-fenced failure.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task RingRefusalIsAttemptedOnce(bool mismatch, CancellationToken cancellationToken)
    {
        await using var policy = new ServerCallPolicy(
            new ServerCallPolicyInstrumentation(new ServerCallPolicyMetrics(_testMeter), new ServerRpcTimeoutMetrics(_testMeter)),
            3,
            64,
            "peer-ring",
            TimeProvider.System,
            new CallPolicyTimeouts());
        int[] attempts = [0];

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException, int>(
            policy.ExecuteAsync(
                (Mismatch: mismatch, Attempts: attempts),
                static (state, _) =>
                {
                    state.Attempts[0]++;
                    return ValueTask.FromException<int>(state.Mismatch ? RingMismatchFailure.Mismatch() : RingMismatchFailure.Fenced());
                },
                cancellationToken));

        _ = await Assert.That(RingMismatchFailure.IsRefusal(ex)).IsTrue();
        _ = await Assert.That(attempts[0]).IsEqualTo(1);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();
}
