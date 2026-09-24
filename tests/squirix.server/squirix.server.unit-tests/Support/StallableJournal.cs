using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Threading;

namespace Squirix.Server.UnitTests.Support;

/// <summary>A real <see cref="JournalCoordinator" /> over a <see cref="StallableJournalSegmentWriter" />, with a WAL replay view for divergence checks.</summary>
[Mutable]
internal sealed class StallableJournal : IAsyncDisposable
{
    private readonly string _dataDir;
    private int _journalDisposed;
    private int _disposed;
    private int _writerDisposed;

    private StallableJournal(string dataDir, Ledger ledger, StallableJournalSegmentWriter writer, JournalCoordinator journal)
    {
        _dataDir = dataDir;
        Ledger = ledger;
        Writer = writer;
        Journal = journal;
    }

    /// <summary>Gets the manifest store the journal publishes to.</summary>
    internal Ledger Ledger { get; }

    /// <summary>Gets the journal under test.</summary>
    internal JournalCoordinator Journal { get; }

    /// <summary>Gets the stallable segment writer injected into <see cref="Journal" />.</summary>
    internal StallableJournalSegmentWriter Writer { get; }

    /// <summary>Releases every stall, disposes the journal and the manifest store.</summary>
    /// <returns>An asynchronous operation.</returns>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        try
        {
            await ShutdownAsync();
        }
        finally
        {
            Ledger.Dispose();
        }
    }

    /// <summary>Creates a journal over a fresh stallable writer; the periodic flush is effectively off so only explicit durability work fsyncs.</summary>
    /// <param name="dataDir">Empty journal data directory.</param>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The started journal.</returns>
    internal static Task<StallableJournal> CreateAsync(string dataDir, bool groupCommit, CancellationToken cancellationToken) =>
        CreateAsync(dataDir, groupCommit ? TimeSpan.FromMilliseconds(20) : TimeSpan.Zero, 1, cancellationToken);

    /// <summary>Creates a journal over a fresh stallable writer with explicit group commit batching.</summary>
    /// <param name="dataDir">Empty journal data directory.</param>
    /// <param name="groupCommitMaxWait">Group commit batch deadline; <see cref="TimeSpan.Zero" /> disables group commit.</param>
    /// <param name="groupCommitMaxBatch">Waiter count that makes a group commit batch due before its deadline.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The started journal.</returns>
    internal static Task<StallableJournal> CreateAsync(string dataDir, TimeSpan groupCommitMaxWait, int groupCommitMaxBatch, CancellationToken cancellationToken) =>
        CreateCoreAsync(dataDir, groupCommitMaxWait, groupCommitMaxBatch, null, cancellationToken);

    /// <summary>
    /// Creates a journal whose disposal gives up on a stuck journal thread after <paramref name="shutdownBudget" />; the grace join
    /// floor is the same budget, so a leaked disposal returns within about twice the budget.
    /// </summary>
    /// <param name="dataDir">Empty journal data directory.</param>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="shutdownBudget">Shared shutdown budget and grace join floor.</param>
    /// <param name="log">Logger of the journal coordinator.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The started journal.</returns>
    internal static Task<StallableJournal> CreateAsync(string dataDir, bool groupCommit, TimeSpan shutdownBudget, ILogger log, CancellationToken cancellationToken) =>
        CreateCoreAsync(dataDir, groupCommit ? TimeSpan.FromMilliseconds(20) : TimeSpan.Zero, 1, (shutdownBudget, log), cancellationToken);

    /// <summary>Waits up to <paramref name="window" /> for <paramref name="signal" /> to complete.</summary>
    /// <param name="signal">Signal to observe.</param>
    /// <param name="window">Longest wait; only a failing (blocked) path pays it.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true" /> when the signal completed inside the window.</returns>
    internal static async Task<bool> CompletesWithinAsync(TaskCompletionSource signal, TimeSpan window, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(signal);
        try
        {
            await signal.Task.WaitAsync(window, TimeProvider.System, cancellationToken);
            return true;
        }
        catch (TimeoutException)
        {
            return false;
        }
    }

    /// <summary>Formats a key set as a sorted, comma-separated list so state comparisons print readable diffs.</summary>
    /// <param name="keys">Keys to format.</param>
    /// <returns>The canonical text form.</returns>
    internal static string Describe(IEnumerable<string> keys)
    {
        var sorted = new List<string>(keys);
        sorted.Sort(StringComparer.Ordinal);
        return string.Join(',', sorted);
    }

    /// <summary>Disposes the journal without releasing the stalls, as a host shutdown over a disk that never returns.</summary>
    /// <returns>The journal disposal.</returns>
    internal Task DisposeStalledAsync()
    {
        _ = Interlocked.Exchange(ref _journalDisposed, 1);
        return Journal.DisposeAsync().AsTask();
    }

    /// <summary>
    /// Releases the stalls after <see cref="DisposeStalledAsync" /> leaked the journal thread, joins it and disposes the writer the
    /// leaked disposal left open, so the segments can be replayed and the data directory deleted.
    /// </summary>
    /// <param name="joinTimeout">Longest wait for the released journal thread to exit.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="TimeoutException">The journal thread did not exit within <paramref name="joinTimeout" />.</exception>
    internal async Task ReclaimLeakedAsync(TimeSpan joinTimeout)
    {
        Writer.ReleaseAll();
        if (!await Journal.DurabilityPipeline.TryJoinJournalThreadAsync(joinTimeout))
            throw new TimeoutException("released journal thread did not exit.");

        if (Interlocked.Exchange(ref _writerDisposed, 1) == 0)
            Writer.Dispose();
    }

    /// <summary>
    /// Rebuilds the key set a restart would recover: <paramref name="snapshot" /> plus every WAL put/remove with a sequence above
    /// <paramref name="afterSequence" />. Call <see cref="ShutdownAsync" /> or <see cref="ReclaimLeakedAsync" /> first.
    /// </summary>
    /// <param name="snapshot">Snapshot key set in <see cref="Describe" /> form; empty for a journal-only restart.</param>
    /// <param name="afterSequence">Last sequence covered by <paramref name="snapshot" />.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The recovered key set in <see cref="Describe" /> form.</returns>
    internal string Recover(string snapshot, ulong afterSequence, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        var keys = new HashSet<string>(snapshot.Split(',', StringSplitOptions.RemoveEmptyEntries), StringComparer.Ordinal);
        using var records = JournalReadPath.ReadAll(_dataDir, 1, cancellationToken);
        while (records.MoveNext())
        {
            var record = records.Current;
            if (record.Sequence <= afterSequence)
                continue;

            if (record.Operation == JournalOperationKind.Put)
                _ = keys.Add(record.Key.ToString());
            else if (record.Operation == JournalOperationKind.Remove)
                _ = keys.Remove(record.Key.ToString());
        }

        return Describe(keys);
    }

    /// <summary>Releases every stall and shuts the journal down so its segments can be replayed.</summary>
    /// <returns>An asynchronous operation.</returns>
    internal async Task ShutdownAsync()
    {
        // A writer reclaimed after a leaked disposal already released and disposed its stalls.
        if (Volatile.Read(ref _writerDisposed) == 0)
            Writer.ReleaseAll();

        if (Interlocked.Exchange(ref _journalDisposed, 1) == 0)
            await Journal.DisposeAsync();
    }

    private static async Task<StallableJournal> CreateCoreAsync(
        string dataDir,
        TimeSpan groupCommitMaxWait,
        int groupCommitMaxBatch,
        (TimeSpan Budget, ILogger Log)? shutdown,
        CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions
        {
            DataDir = dataDir,
            JournalMaxSegmentMb = 4,
            FlushInterval = 600_000,
            ManifestRetentionCount = 1,
            JournalGroupCommitMaxWait = groupCommitMaxWait,
            JournalGroupCommitMaxBatch = groupCommitMaxBatch,
        };
        var ledger = new Ledger(options);
        var writer = new StallableJournalSegmentWriter();
        try
        {
            var manifest = await ledger.ReadCurrentOrDefaultAsync(cancellationToken);
            var journal = shutdown is { } stuck
                ? new JournalCoordinator(options, manifest, ledger, new AsyncManualResetEvent(true), writer, stuck.Log)
                {
                    ShutdownBudget = stuck.Budget,
                    GraceJoinFloor = stuck.Budget,
                }
                : new JournalCoordinator(options, manifest, ledger, new AsyncManualResetEvent(true), writer);
            return new StallableJournal(dataDir, ledger, writer, journal);
        }
        catch
        {
            writer.Dispose();
            ledger.Dispose();
            throw;
        }
    }
}
