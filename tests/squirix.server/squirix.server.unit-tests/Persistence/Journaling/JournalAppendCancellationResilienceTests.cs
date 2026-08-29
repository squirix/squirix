using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Diagnostics;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>
/// Regression coverage for the append cancellation lifecycle (audit items C1 and C2): cancelling a
/// request after its frame is enqueued must not corrupt the durability ack pool or double-decrement
/// the queued-append counter, and durable group commits must not starve across a segment roll.
/// </summary>
[Immutable]
public sealed class JournalAppendCancellationResilienceTests : IsolatedStorageTestBase
{
    /// <summary>
    /// Cancelling many durable group-commit mutations around their enqueue boundary leaves the
    /// coordinator healthy: a subsequent clean durable mutation still commits and the pipeline disposes
    /// without hanging.
    /// </summary>
    [Fact]
    public async Task CanceledGroupCommitKeepsPipelineHealthy()
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 4,
            FlushInterval = 600_000,
            ManifestRetentionCount = 1,
            JournalGroupCommitMaxWait = TimeSpan.FromMilliseconds(2),
            JournalGroupCommitMaxBatch = 8,
        };

        using var manifestStore = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
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
    public async Task GroupCommitCompletesAcrossRoll()
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 1,
            FlushInterval = 600_000,
            ManifestRetentionCount = 1,
            JournalGroupCommitMaxWait = TimeSpan.FromMilliseconds(5),
            JournalGroupCommitMaxBatch = 8,
        };

        using var manifestStore = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        var pipelined = Assert.IsType<JournalCoordinator>(journal);

        const int payloadSize = 16_000;
        var payload = new byte[payloadSize];
        Array.Fill(payload, Convert.ToByte('z'));

        var deadline = Environment.TickCount64 + 30_000;
        for (var i = 0; pipelined.CurrentSegmentIndex == 1 && Environment.TickCount64 < deadline;)
        {
            await journal.AppendPutAsync(CacheKey.Default(NodeInvariantIndexStrings.Format(i)), payload, DefaultCancellationToken);
            await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken).AsTask().WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, DefaultCancellationToken);
            i++;
        }

        Assert.Equal(2, pipelined.CurrentSegmentIndex);
    }

    private static async Task AppendIgnoringCancellationAsync(IJournalCoordinator journal, CacheKey key, byte[] payload, int cancelAfterMs)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(cancelAfterMs));
        try
        {
            await journal.AppendPutAndAwaitDurabilityAsync(key, payload, cts.Token);
        }
        catch (OperationCanceledException ex)
        {
            TestLog.Suppressed("Append cancellation observed on the pre-enqueue backpressure path; expected, shared state must remain intact.", ex);
        }
    }

    private static Task RunCancellationStormAsync(IJournalCoordinator journal, byte[] payload, int iterations, CancellationToken cancellationToken)
    {
        var tasks = new Task[iterations];
        for (var i = 0; i < iterations; i++)
        {
            var key = CacheKey.Default(NodeInvariantIndexStrings.Format(i));

            // Use 1..4 ms (not 0): a zero due-time CTS is already canceled and only exercises the
            // pre-enqueue path, which is less representative of the durability-ack race this test
            // was written to catch under CI scheduling pressure.
            tasks[i] = AppendIgnoringCancellationAsync(journal, key, payload, 1 + (i % 4));
        }

        return Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(60), TimeProvider.System, cancellationToken);
    }
}
