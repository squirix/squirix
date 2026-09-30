using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>
/// A hosted node measures time with the clock registered in its container: journal compaction, admission rate limiting, the
/// internode call policy and the journal stall probe all run on it, so a test clock drives them without real delays.
/// </summary>
public sealed class ServerClockTests : NodeIntegrationTestBase
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    /// <summary>The compaction loop parks its wait between checks on the server clock and takes its next turn only when that clock advances.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactionWaitsOnServerClock(CancellationToken cancellationToken)
    {
        // The node arms the compaction wait at MinGap (two minutes) plus or minus a jitter capped at ten seconds; no other timer is that long.
        var clock = new TimerRecordingClock(TimeSpan.FromSeconds(110), TimeSpan.FromSeconds(130));
        await using var cluster = await StartClusterAsync("node_clock_compaction", new IntegrationStartOptions { UsePersistence = true, TimeProvider = clock }, cancellationToken);

        await clock.WaitForTimersAsync(1, cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(130));
        await clock.WaitForTimersAsync(2, cancellationToken);

        _ = await Assert.That(clock.RecordedTimers).IsGreaterThanOrEqualTo(2);
    }

    /// <summary>The node rate limiter rejects once its burst is spent and admits again after the server clock advances one second.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NodeRateLimitRefillsOnServerClock(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var admission = new AdmissionOptions
        {
            MaxInFlight = 4,
            MaxQueue = 0,
            SlowdownThreshold = 4,
            RejectThreshold = 4,
            MaxSlowdownDelay = TimeSpan.Zero,
            NodeRateLimitPerSecond = 1,
            NodeRateLimitBurst = 1,
        };
        await using var cluster = await StartClusterAsync("node_clock_rate", new IntegrationStartOptions { BackpressureOptions = admission, TimeProvider = clock }, cancellationToken);
        var gate = cluster["node_clock_rate"].GetRequiredService<IBackpressureGate>();

        var (first, firstLease) = await gate.AcquireAsync("grpc", "get", "grpc:client-a", cancellationToken);
        firstLease.Dispose();
        var (limited, limitedLease) = await gate.AcquireAsync("grpc", "get", "grpc:client-a", cancellationToken);
        limitedLease.Dispose();
        clock.Advance(TimeSpan.FromSeconds(1));
        var (refilled, refilledLease) = await gate.AcquireAsync("grpc", "get", "grpc:client-a", cancellationToken);
        refilledLease.Dispose();

        _ = await Assert.That(first.IsAccepted).IsTrue();
        _ = await Assert.That(limited.RejectReason).IsEqualTo("node_rate_limit");
        _ = await Assert.That(refilled.IsAccepted).IsTrue();
    }

    /// <summary>The internode call policy the node builds parks its retry backoff on the server clock.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PeerRetryBackoffParksOnServerClock(CancellationToken cancellationToken)
    {
        var clock = new CallerTimerClock();
        await using var cluster = await StartClusterAsync("node_clock_a", "node_clock_b", new IntegrationStartOptions { TimeProvider = clock }, cancellationToken);
        var policy = cluster["node_clock_a"].GetRequiredService<IServerClientPool>().PolicyFor("node_clock_b");

        var attempts = new InvocationCounter();
        Task<int> execute;
        using (clock.RecordCaller())
        {
            execute = policy.ExecuteAsync(
                attempts,
                static (counter, _) => counter.Increment() == 1 ? ValueTask.FromException<int>(new RpcException(new Status(StatusCode.Unavailable, "down"))) : new ValueTask<int>(7),
                cancellationToken).AsTask();
        }

        // The first attempt fails synchronously, so the call already parked its backoff on a server clock timer.
        var backoffTimers = clock.RecordedTimers;
        clock.Advance(TimeSpan.FromMinutes(1));
        var result = await execute.WaitAsync(Bound, TimeProvider.System, cancellationToken);

        _ = await Assert.That(backoffTimers).IsEqualTo(1);
        _ = await Assert.That(result).IsEqualTo(7);
        _ = await Assert.That(attempts.Count).IsEqualTo(2);
    }

    /// <summary>A journal I/O call in progress degrades readiness once the server clock passes the stall threshold, with no real stall.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The host journal does not expose its stall probe.</exception>
    [Test]
    public async Task StallDegradesReadinessOnServerClock(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        await using var cluster = await StartClusterAsync("node_clock_stall", new IntegrationStartOptions { UsePersistence = true, TimeProvider = clock }, cancellationToken);
        var node = cluster["node_clock_stall"];
        if (node.GetRequiredService<JournalCoordinatorHost>().Coordinator is not IJournalStallProbeSource journal)
            throw new InvalidOperationException("the host journal does not expose its stall probe.");

        // The node is idle, so its journal thread performs no segment I/O and the test is the only writer of the probe.
        journal.StallProbe.IoStarted(nameof(IJournalSegmentWriter.FlushToDisk));
        (HttpStatusCode Status, string Body) fresh;
        (HttpStatusCode Status, string Body) stalled;
        try
        {
            fresh = await GetReadyAsync(node.Uri, cancellationToken);
            clock.Advance(new Storage.PersistenceOptions().JournalStallDegradedThreshold);
            stalled = await GetReadyAsync(node.Uri, cancellationToken);
        }
        finally
        {
            journal.StallProbe.IoFinished();
        }

        _ = await Assert.That(fresh).IsEqualTo((HttpStatusCode.OK, nameof(HealthStatus.Healthy)));
        _ = await Assert.That(stalled).IsEqualTo((HttpStatusCode.OK, nameof(HealthStatus.Degraded)));
    }

    private async Task<(HttpStatusCode Status, string Body)> GetReadyAsync(Uri uri, CancellationToken cancellationToken)
    {
        using var response = await HttpClient.GetAsync(new Uri(uri, "/health/ready"), cancellationToken);
        return (response.StatusCode, await response.Content.ReadAsStringAsync(cancellationToken));
    }

    private sealed class InvocationCounter
    {
        private int _count;

        internal int Count => Volatile.Read(ref _count);

        internal int Increment() => Interlocked.Increment(ref _count);
    }

    /// <summary>A fake clock that counts the timers created with a due time inside one window.</summary>
    [ThreadSafe]
    private sealed class TimerRecordingClock : FakeTimeProvider
    {
        private readonly TimeSpan _maxDue;
        private readonly TimeSpan _minDue;
        private readonly SemaphoreSlim _recorded = new(0);
        private int _count;

        internal TimerRecordingClock(TimeSpan minDue, TimeSpan maxDue)
            : base(DateTimeOffset.UtcNow)
        {
            _minDue = minDue;
            _maxDue = maxDue;
        }

        internal int RecordedTimers => Volatile.Read(ref _count);

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            if (dueTime < _minDue || dueTime > _maxDue)
                return base.CreateTimer(callback, state, dueTime, period);

            _ = Interlocked.Increment(ref _count);
            _ = _recorded.Release();
            return base.CreateTimer(callback, state, dueTime, period);
        }

        /// <summary>Waits until at least <paramref name="count" /> timers were recorded, bounded in real time.</summary>
        /// <param name="count">The number of recorded timers to wait for.</param>
        /// <param name="cancellationToken">The test cancellation token.</param>
        /// <returns>A task that completes once the timers were recorded.</returns>
        /// <exception cref="TimeoutException">No further timer was recorded within the real-time bound.</exception>
        internal async Task WaitForTimersAsync(int count, CancellationToken cancellationToken)
        {
            while (RecordedTimers < count)
            {
                if (!await _recorded.WaitAsync(Bound, cancellationToken))
                    throw new TimeoutException($"only {RecordedTimers} of {count} timers were created in the due window.");
            }
        }
    }

    /// <summary>A fake clock that counts the timers created by the call flow inside <see cref="RecordCaller" />.</summary>
    [ThreadSafe]
    private sealed class CallerTimerClock : FakeTimeProvider
    {
        private readonly AsyncLocal<bool> _recording = new();
        private int _count;

        internal CallerTimerClock()
            : base(DateTimeOffset.UtcNow)
        {
        }

        internal int RecordedTimers => Volatile.Read(ref _count);

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            if (_recording.Value)
                _ = Interlocked.Increment(ref _count);

            return base.CreateTimer(callback, state, dueTime, period);
        }

        /// <summary>Records the timers the current call flow creates until the scope is disposed.</summary>
        /// <returns>The scope that stops recording.</returns>
        internal RecordingScope RecordCaller()
        {
            _recording.Value = true;
            return new RecordingScope(_recording);
        }

        internal readonly record struct RecordingScope(AsyncLocal<bool> Recording) : IDisposable
        {
            public void Dispose() => Recording.Value = false;
        }
    }
}
