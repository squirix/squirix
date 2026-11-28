using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Internal.Cluster.Observability;
using Squirix.Internal.Cluster.Reliability;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests.Cluster;

/// <summary>Covers queue wait measurements across listener changes.</summary>
public sealed class CallPolicyQueueWaitTests
{
    /// <summary>A listener attached after a request queues receives its measurement on admission.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LateListenerReceivesQueuedCall(CancellationToken cancellationToken)
    {
        const string peer = "late-queue-wait-listener";
        await using var policy = new CallPolicy(TimeProvider.System, TimeSpan.FromMinutes(1), maxConcurrentPerPeer: 1, peer: peer);
        var release = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var holding = policy.ExecuteAsync(static (gate, token) => new ValueTask<int>(gate.Task.WaitAsync(token)), release, cancellationToken);
        var queued = policy.ExecuteAsync(static (result, _) => ValueTask.FromResult(result), 2, cancellationToken);
        using var listener = new MeterListener
        {
            InstrumentPublished = static (instrument, current) =>
            {
                if (ReferenceEquals(instrument.Meter, MeterRegistry.Meter) && string.Equals(instrument.Name, "squirix_call_policy_queue_wait_seconds", StringComparison.Ordinal))
                    current.EnableMeasurementEvents(instrument);
            },
        };
        var samples = 0;
        listener.SetMeasurementEventCallback<double>((_, duration, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (string.Equals(tag.Key, "peer", StringComparison.Ordinal) && string.Equals(tag.Value as string, peer, StringComparison.Ordinal) && duration > 0)
                    _ = Interlocked.Increment(ref samples);
            }
        });

        try
        {
            _ = await Assert.That(queued.IsCompleted).IsFalse();
            listener.Start();
        }
        finally
        {
            _ = release.TrySetResult(1);
        }

        _ = await holding;
        _ = await queued;
        _ = await Assert.That(Volatile.Read(ref samples)).IsEqualTo(1);
    }
}
