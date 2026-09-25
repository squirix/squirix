using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>
/// Host disposal over a journal whose dispose fails (a leaked journal I/O thread): the journal host logs the failure and the container keeps
/// disposing the persistence runtime instead of aborting.
/// </summary>
[Immutable]
public sealed class PersistenceDisposeTests : IsolatedStorageTestBase
{
    private const int JournalDisposeFailedEventId = 3016;

    /// <summary>A throwing journal dispose neither escapes the container nor skips the manifest ledger.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LedgerDisposedWhenJournalDisposeThrows(CancellationToken cancellationToken)
    {
        using var meter = new Meter("test-persistence-dispose");
        var provider = await BuildProviderAsync(meter, null, cancellationToken);
        var ledger = provider.GetRequiredService<Ledger>();
        _ = AttachThrowingJournal(provider);

        await provider.DisposeAsync();

        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(ledger.ReadCurrentOrDefaultAsync(cancellationToken));
    }

    /// <summary>A throwing journal dispose is reported at error level with the journal failure as its cause.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task JournalDisposeFailureLoggedAsError(CancellationToken cancellationToken)
    {
        using var meter = new Meter("test-persistence-dispose-log");
        var log = new EventRecordingLogger();
        var provider = await BuildProviderAsync(meter, log, cancellationToken);
        var journal = AttachThrowingJournal(provider);

        await provider.DisposeAsync();

        var entry = log.Find(JournalDisposeFailedEventId);

        _ = await Assert.That(entry?.Level).IsEqualTo(LogLevel.Error);
        _ = await Assert.That(entry?.Cause).IsSameReferenceAs(journal.Failure);
    }

    private static ThrowingDisposeJournal AttachThrowingJournal(IServiceProvider provider)
    {
        var host = provider.GetRequiredService<JournalCoordinatorHost>();
        var journal = new ThrowingDisposeJournal(host.Coordinator);
        host.Attach(journal);
        return journal;
    }

    private async Task<ServiceProvider> BuildProviderAsync(Meter meter, EventRecordingLogger? log, CancellationToken cancellationToken)
    {
        var services = new ServiceCollection();
        if (log != null)
            _ = services.AddLogging(builder => builder.AddProvider(new RecordingLoggerProvider(log)));

        _ = await services.AddPersistenceServicesAsync(new PersistenceOptions { DataDir = Dir }, meter, false, cancellationToken);
        return services.BuildServiceProvider();
    }

    /// <summary>Logger provider handing out one recording logger for every category.</summary>
    [Immutable]
    private sealed class RecordingLoggerProvider : ILoggerProvider
    {
        private readonly EventRecordingLogger _log;

        internal RecordingLoggerProvider(EventRecordingLogger log)
        {
            _log = log;
        }

        public ILogger CreateLogger(string categoryName) => _log;

        public void Dispose()
        {
        }
    }

    /// <summary>Journal double that disposes the real journal and then fails once, as a journal whose I/O thread leaked on shutdown does.</summary>
    [ThreadSafe]
    private sealed class ThrowingDisposeJournal : IJournalCoordinator
    {
        private readonly IJournalCoordinator _inner;
        private int _disposed;

        internal ThrowingDisposeJournal(IJournalCoordinator inner)
        {
            _inner = inner;
        }

        public event EventHandler? OnAppended
        {
            add => _ = value;
            remove => _ = value;
        }

        public long AppendedBytes => 0;

        public long AppendedOps => 0;

        public int CurrentSegmentIndex => 0;

        public bool HasFlushLoopFailure => false;

        public long HighWaterBytes => 0;

        public QuiescenceGate InFlightApplyGate => _inner.InFlightApplyGate;

        public bool IsJournalGroupCommitEnabled => false;

        public long MaxBytes => 0;

        public ulong NextSequence => 0;

        public double RecentAppendLatencyMs => 0;

        public long UsedBytes => 0;

        internal TimeoutException Failure { get; } = new("journal I/O thread is still alive after shutdown; writer, ring, and gates are leaked.");

        public ValueTask AppendIdempotencyOutcomeAsync(string operationId, string fingerprint, byte[] responseBytes, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask AppendPutAndAwaitDurabilityAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask AppendPutAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask AppendRemoveAsync(CacheKey key, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask AppendRemoveExpirationAsync(CacheKey key, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask AppendTouchExpirationAsync(CacheKey key, DateTime expiresUtc, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask AwaitDurabilityCommitAsync(CancellationToken cancellationToken) => throw new NotSupportedException();

        public async ValueTask DisposeAsync()
        {
            await _inner.DisposeAsync();
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
                throw Failure;
        }

        public ValueTask ExecuteMaintenanceExclusiveAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<TResult> ExecuteSnapshotCutAsync<TState, TBarrier, TResult>(
            TState state,
            Func<TState, ulong, CancellationToken, ValueTask<TBarrier>> captureUnderBarrier,
            Func<TState, ulong, TBarrier, CancellationToken, ValueTask<TResult>> buildOutsideBarrier,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TResult>(Func<CancellationToken, ValueTask<TResult>> action, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TState, TResult>(
            TState state,
            Func<TState, CancellationToken, ValueTask<TResult>> action,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        public ValueTask ExecuteUnderSnapshotBarrierAsync<TState>(TState state, Func<TState, CancellationToken, ValueTask> action, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public void FailJournalPipeline(Exception reason) => throw new NotSupportedException();

        public Exception? GetJournalThreadFailure() => null;

        public ValueTask WaitForStartupAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }
}
