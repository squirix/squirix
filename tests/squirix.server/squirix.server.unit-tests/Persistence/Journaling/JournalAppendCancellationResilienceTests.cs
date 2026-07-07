using System;
using System.Buffers;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Journaling;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>
/// Regression coverage for the append cancellation lifecycle (audit items C1 and C2): cancelling a
/// request after its frame is enqueued must not corrupt the durability waiter pool or double-decrement
/// the queued-append counter, and durable group commits must not starve across a segment roll.
/// </summary>
public sealed class JournalAppendCancellationResilienceTests : UnitTestBase
{
    /// <summary>
    /// Cancelling many durable group-commit mutations around their enqueue boundary leaves the
    /// coordinator healthy: a subsequent clean durable mutation still commits and the pipeline disposes
    /// without hanging.
    /// </summary>
    [Fact]
    public async Task CancellingDurableGroupCommitsAroundEnqueueKeepsPipelineHealthy()
    {
        using var dir = new TempDirectory("squirix-journal-cancel-storm");
        var options = new PersistenceOptions
        {
            DataDir = dir,
            JournalMaxSegmentMb = 4,
            FlushIntervalMs = 600_000,
            ManifestRetentionCount = 1,
            JournalGroupCommitMaxWait = TimeSpan.FromMilliseconds(2),
            JournalGroupCommitMaxBatch = 8,
        };

        using var manifestStore = new ManifestStore(options);
        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);
        await journal.WaitForStartupAsync(DefaultCancellationToken);

        const int iterations = 256;
        var payload = JournalEntryPayloadKit.EncodePut("v");
        await RunCancellationStormAsync(journal, payload, iterations, DefaultCancellationToken);

        var opsBefore = journal.AppendedOps;

        // The pool/counter must still be intact: a clean durable mutation completes promptly.
        await journal.AppendPutAndAwaitDurabilityAsync(CacheKey.Default("final"), payload, DefaultCancellationToken).AsTask()
                     .WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, DefaultCancellationToken);

        Assert.True(journal.AppendedOps > opsBefore);
    }

    /// <summary>
    /// A durable group commit issued across a forced segment roll completes (it is not starved for the
    /// whole roll), and the journal rolls to the next segment.
    /// </summary>
    [Fact]
    public async Task DurableGroupCommitCompletesAcrossSegmentRoll()
    {
        using var dir = new TempDirectory("squirix-journal-gc-roll");
        var options = new PersistenceOptions
        {
            DataDir = dir,
            JournalMaxSegmentMb = 1,
            FlushIntervalMs = 600_000,
            ManifestRetentionCount = 1,
            JournalGroupCommitMaxWait = TimeSpan.FromMilliseconds(5),
            JournalGroupCommitMaxBatch = 8,
        };

        using var manifestStore = new ManifestStore(options);
        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);
        var pipelined = Assert.IsType<JournalCoordinator>(journal);

        const int payloadSize = 16_000;
        var payload = ArrayPool<byte>.Shared.Rent(payloadSize);
        try
        {
            Array.Fill(payload, Convert.ToByte('z'), 0, payloadSize);

            var deadline = Environment.TickCount64 + 30_000;
            for (var i = 0; pipelined.CurrentSegmentIndex is 1 && Environment.TickCount64 < deadline;)
            {
                await journal.AppendPutAsync(CacheKey.Default($"k{i.ToString(CultureInfo.InvariantCulture)}"), payload.AsMemory(0, payloadSize), DefaultCancellationToken);
                await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken).AsTask().WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, DefaultCancellationToken);
                i++;
            }

            Assert.Equal(2, pipelined.CurrentSegmentIndex);
            Assert.False(pipelined.EventLoop.IsDurabilityFlushPending);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(payload);
        }
    }

    private static async Task AppendIgnoringCancellationAsync(IJournalCoordinator journal, CacheKey key, byte[] payload, int cancelAfterMs)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(cancelAfterMs));
        try
        {
            await journal.AppendPutAndAwaitDurabilityAsync(key, payload, cts.Token);
        }
        catch (OperationCanceledException)
        {
            // Expected for the pre-enqueue backpressure path; must not corrupt shared state.
        }
    }

    private static Task RunCancellationStormAsync(IJournalCoordinator journal, byte[] payload, int iterations, CancellationToken cancellationToken)
    {
        var tasks = new Task[iterations];
        for (var i = 0; i < iterations; i++)
        {
            var key = CacheKey.Default($"k{i.ToString(CultureInfo.InvariantCulture)}");
            tasks[i] = AppendIgnoringCancellationAsync(journal, key, payload, i % 4);
        }

        return Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
    }
}
