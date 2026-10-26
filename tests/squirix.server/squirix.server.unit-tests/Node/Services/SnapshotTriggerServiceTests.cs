using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Verifies a failed snapshot attempt backs off and keeps the host running, and that only a failure the node cannot outlive latches the service.</summary>
[Immutable]
public sealed class SnapshotTriggerServiceTests : IsolatedStorageTestBase
{
    private const string TooLargeMessage = "snapshot too large";

    private static readonly TimeSpan Tick = TimeSpan.FromSeconds(1);

    /// <summary>A snapshot that fails with invalid data leaves the task running, reports the failure and tries again only after the backoff.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InvalidDataFailureBacksOffAndRetries(CancellationToken cancellationToken)
    {
        using var harness = await Harness.CreateAsync(Dir, new InvalidDataException(TooLargeMessage), cancellationToken);
        await harness.Service.StartAsync(cancellationToken);
        try
        {
            await harness.AdvanceUntilAsync(static h => h.Service.CurrentFailure != null, cancellationToken);

            var failure = harness.Service.CurrentFailure;
            _ = await Assert.That(failure!.ExceptionType).IsEqualTo(nameof(InvalidDataException));
            _ = await Assert.That(failure.Message).IsEqualTo(TooLargeMessage);
            _ = await Assert.That(failure.ConsecutiveFailures).IsEqualTo(1);
            _ = await Assert.That(harness.Service.HasFatalFailure).IsFalse();
            _ = await Assert.That(harness.Service.ExecuteTask!.IsCompleted).IsFalse();

            // The backoff is at least one millisecond of the fake clock, which no one has advanced since the failure.
            await Task.Yield();
            _ = await Assert.That(harness.Attempts).IsEqualTo(1);

            await harness.AdvanceUntilAsync(static h => h.Attempts >= 2, cancellationToken);
            _ = await Assert.That(harness.Service.HasFatalFailure).IsFalse();
        }
        finally
        {
            await harness.Service.StopAsync(CancellationToken.None);
        }
    }

    /// <summary>An I/O failure is retriable too.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task IoFailureKeepsTaskRunning(CancellationToken cancellationToken)
    {
        using var harness = await Harness.CreateAsync(Dir, new IOException("disk full"), cancellationToken);
        await harness.Service.StartAsync(cancellationToken);
        try
        {
            await harness.AdvanceUntilAsync(static h => h.Service.CurrentFailure != null, cancellationToken);

            _ = await Assert.That(harness.Service.CurrentFailure!.ExceptionType).IsEqualTo(nameof(IOException));
            _ = await Assert.That(harness.Service.HasFatalFailure).IsFalse();
            _ = await Assert.That(harness.Service.ExecuteTask!.IsCompleted).IsFalse();
        }
        finally
        {
            await harness.Service.StopAsync(CancellationToken.None);
        }
    }

    /// <summary>A cancellation the host did not request is a failed attempt, not a stop.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnrequestedCancellationKeepsTaskRunning(CancellationToken cancellationToken)
    {
        using var harness = await Harness.CreateAsync(Dir, new OperationCanceledException("linked timeout"), cancellationToken);
        await harness.Service.StartAsync(cancellationToken);
        try
        {
            await harness.AdvanceUntilAsync(static h => h.Service.CurrentFailure != null, cancellationToken);

            _ = await Assert.That(harness.Service.CurrentFailure!.ExceptionType).IsEqualTo(nameof(OperationCanceledException));
            _ = await Assert.That(harness.Service.HasFatalFailure).IsFalse();
            _ = await Assert.That(harness.Service.ExecuteTask!.IsCompleted).IsFalse();
        }
        finally
        {
            await harness.Service.StopAsync(CancellationToken.None);
        }
    }

    /// <summary>A snapshot that succeeds after failures clears the failure and the count.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SuccessClearsFailure(CancellationToken cancellationToken)
    {
        using var harness = await Harness.CreateAsync(Dir, new IOException("disk full"), cancellationToken);
        await harness.Service.StartAsync(cancellationToken);
        try
        {
            await harness.AdvanceUntilAsync(static h => h.Service.CurrentFailure != null, cancellationToken);
            harness.Recover();

            await harness.AdvanceUntilAsync(static h => h.Succeeded && h.Service.CurrentFailure == null, cancellationToken);

            _ = await Assert.That(harness.Service.HasFatalFailure).IsFalse();
            _ = await Assert.That(harness.Service.ExecuteTask!.IsCompleted).IsFalse();
        }
        finally
        {
            await harness.Service.StopAsync(CancellationToken.None);
        }
    }

    /// <summary>A tick the triggers skip publishes nothing and leaves the failure reported; only a published snapshot clears it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SkippedTickKeepsFailure(CancellationToken cancellationToken)
    {
        const int SkippedTicks = 20;
        using var harness = await Harness.CreateAsync(Dir, new IOException("disk full"), cancellationToken);
        await harness.Service.StartAsync(cancellationToken);
        try
        {
            await harness.AdvanceUntilAsync(static h => h.Service.CurrentFailure != null, cancellationToken);
            harness.Journal.AppendedOps = 0;
            harness.Journal.AppendedBytes = 0;
            var ticks = 0;

            await harness.AdvanceUntilAsync(_ => ++ticks >= SkippedTicks, cancellationToken);

            _ = await Assert.That(harness.Service.CurrentFailure).IsNotNull();
            _ = await Assert.That(harness.Attempts).IsEqualTo(1);
            _ = await Assert.That(harness.Succeeded).IsFalse();

            harness.Journal.AppendedOps = 1;
            harness.Journal.AppendedBytes = 1024;
            harness.Recover();
            await harness.AdvanceUntilAsync(static h => h.Succeeded && h.Service.CurrentFailure == null, cancellationToken);
        }
        finally
        {
            await harness.Service.StopAsync(CancellationToken.None);
        }
    }

    /// <summary>A failure while the journal pipeline is latched ends the loop with the fatal flag and without faulting the task.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LatchedJournalIsFatalWithoutRethrow(CancellationToken cancellationToken)
    {
        using var harness = await Harness.CreateAsync(Dir, new IOException("fsync failed"), cancellationToken);
        harness.Journal.LatchedFailure = new IOException("fsync failed");
        await harness.Service.StartAsync(cancellationToken);
        try
        {
            await harness.AdvanceUntilAsync(static h => h.Service.ExecuteTask!.IsCompleted, cancellationToken);

            _ = await Assert.That(harness.Service.ExecuteTask!.IsCompletedSuccessfully).IsTrue();
            _ = await Assert.That(harness.Service.HasFatalFailure).IsTrue();
            _ = await Assert.That(harness.Service.CurrentFailure!.ExceptionType).IsEqualTo(nameof(IOException));
        }
        finally
        {
            await harness.Service.StopAsync(CancellationToken.None);
        }
    }

    /// <summary>A failure of a type the service does not know ends the loop with the fatal flag and without faulting the task.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnexpectedFailureIsFatalWithoutRethrow(CancellationToken cancellationToken)
    {
        using var harness = await Harness.CreateAsync(Dir, new NotSupportedException("unexpected"), cancellationToken);
        await harness.Service.StartAsync(cancellationToken);
        try
        {
            await harness.AdvanceUntilAsync(static h => h.Service.ExecuteTask!.IsCompleted, cancellationToken);

            _ = await Assert.That(harness.Service.ExecuteTask!.IsCompletedSuccessfully).IsTrue();
            _ = await Assert.That(harness.Service.HasFatalFailure).IsTrue();
            _ = await Assert.That(harness.Service.CurrentFailure!.ExceptionType).IsEqualTo(nameof(NotSupportedException));
        }
        finally
        {
            await harness.Service.StopAsync(CancellationToken.None);
        }
    }

    /// <summary>The gauge reports the consecutive failure count of the status and zero once it is clear.</summary>
    [Test]
    public async Task GaugeReportsConsecutiveFailures()
    {
        using var meter = new Meter("squirix.test.snapshot-gauge");
        var telemetry = new OpenTelemetrySnapshotTelemetry(meter);
        var status = new MutableStatus();
        telemetry.ObserveConsecutiveFailures("node-1", () => status.Failure?.ConsecutiveFailures ?? 0);
        var observed = new List<(int Value, string? Node)>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = static (instrument, meterListener) =>
        {
            if (string.Equals(instrument.Name, "squirix_snapshot_consecutive_failures", StringComparison.Ordinal))
                meterListener.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<int>(
            (_, measurement, tags, _) =>
            {
                string? node = null;
                foreach (var tag in tags)
                {
                    if (string.Equals(tag.Key, "node", StringComparison.Ordinal))
                        node = tag.Value as string;
                }

                observed.Add((measurement, node));
            });
        listener.Start();

        listener.RecordObservableInstruments();
        status.Failure = new SnapshotFailure(nameof(IOException), "disk full", 4, DateTimeOffset.UnixEpoch);
        listener.RecordObservableInstruments();

        _ = await Assert.That(observed.Count).IsEqualTo(2);
        _ = await Assert.That(observed[0]).IsEqualTo((0, "node-1"));
        _ = await Assert.That(observed[1]).IsEqualTo((4, "node-1"));
    }

    [Mutable]
    private sealed class MutableStatus
    {
        private SnapshotFailure? _failure;

        public SnapshotFailure? Failure
        {
            get => Volatile.Read(ref _failure);
            set => Volatile.Write(ref _failure, value);
        }
    }

    private sealed class WriterState
    {
        private readonly Exception _failure;
        private int _attempts;
        private int _recovered;
        private int _succeeded;

        internal WriterState(Exception failure)
        {
            _failure = failure;
        }

        internal int Attempts => Volatile.Read(ref _attempts);

        internal bool Succeeded => Volatile.Read(ref _succeeded) != 0;

        internal void Recover() => Volatile.Write(ref _recovered, 1);

        internal ValueTask<string> WriteSnapshotAsync()
        {
            _ = Interlocked.Increment(ref _attempts);
            if (Volatile.Read(ref _recovered) == 0)
                throw _failure;

            Volatile.Write(ref _succeeded, 1);
            return ValueTask.FromResult("snap-test-path");
        }
    }

    private sealed class Harness : IDisposable
    {
        private readonly Ledger _ledger;
        private readonly WriterState _writer;

        private Harness(Ledger ledger, SnapshotCutJournal journal, FakeTimeProvider clock, SnapshotTriggerService<object?> service, WriterState writer)
        {
            _ledger = ledger;
            Journal = journal;
            Clock = clock;
            Service = service;
            _writer = writer;
        }

        internal int Attempts => _writer.Attempts;

        internal FakeTimeProvider Clock { get; }

        internal SnapshotCutJournal Journal { get; }

        internal SnapshotTriggerService<object?> Service { get; }

        internal bool Succeeded => _writer.Succeeded;

        public void Dispose()
        {
            Service.Dispose();
            _ledger.Dispose();
        }

        internal static async Task<Harness> CreateAsync(string dir, Exception failure, CancellationToken cancellationToken)
        {
            var ledger = new Ledger(new PersistenceOptions { DataDir = dir }, NullLogger<Ledger>.Instance);
            await ledger.WriteAsync(new State { CurrentJournal = 1, NextSequence = 1 }, cancellationToken);
            var options = ServerJsonSerializer.Deserialize<TriggerOptions>("""{"minGapBetweenSnapshots":"00:00:00","snapshotEveryNOps":1}""")!;
            var journal = new SnapshotCutJournal(1, 2);
            var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
            var writer = new WriterState(failure);
            var captureExpectations = new ISnapshotEntryCaptureCreateExpectations();
            _ = captureExpectations.Setups.CaptureEntriesAsync(Arg.Any<List<(CacheKey Key, NodeCacheEntry<object?> Entry)>>(), Arg.Any<DateTime>(), Arg.Any<CancellationToken>())
                                   .ReturnValue(default);
            var exporterExpectations = new IIdempotencySnapshotExporterCreateExpectations();
            _ = exporterExpectations.Setups.ExportSnapshot(Arg.Any<List<PersistedIdempotencyRecord>>(), Arg.Any<DateTime>());
            var writerExpectations = new ISnapshotWriterCreateExpectations();
            _ = writerExpectations.Setups.AllocateIndex(Arg.Any<int>()).Callback(static publishedIndex => publishedIndex + 1);
            _ = writerExpectations.Setups.WriteAsync(
                Arg.Any<int>(),
                Arg.Any<IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)>>(),
                Arg.Any<IReadOnlyList<PersistedIdempotencyRecord>>(),
                Arg.Any<CancellationToken>()).Callback((_, _, _, _) => writer.WriteSnapshotAsync());
            var coordinator = new Coordinator(
                options,
                journal,
                new CoordinatorDependencies(captureExpectations.Instance(), writerExpectations.Instance(), ledger, exporterExpectations.Instance(), "test-node", null),
                clock);
            var service = new SnapshotTriggerService<object?>(NullLogger<SnapshotTriggerService<object?>>.Instance, coordinator, journal, clock);
            return new Harness(ledger, journal, clock, service, writer);
        }

        /// <summary>Advances the fake clock one tick per poll until <paramref name="condition" /> holds; the service sleeps only on that clock.</summary>
        /// <param name="condition">The state to wait for.</param>
        /// <param name="cancellationToken">The test cancellation token.</param>
        /// <returns>A task that completes when the condition holds.</returns>
        internal Task AdvanceUntilAsync(Func<Harness, bool> condition, CancellationToken cancellationToken) =>
            this.WaitUntilAsync(
                harness =>
                {
                    harness.Clock.Advance(Tick);
                    return condition(harness);
                },
                cancellationToken);

        internal void Recover() => _writer.Recover();
    }
}
