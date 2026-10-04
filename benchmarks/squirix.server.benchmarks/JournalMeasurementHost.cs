using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Node.App;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Threading;

namespace Squirix.Server.Benchmarks;

/// <summary>Hosts a production <see cref="JournalCoordinator" /> and <see cref="DurableMutationExecutor" /> over a <see cref="JournalMeasuringSegmentWriter" /> for the measurement runners.</summary>
[Immutable]
internal sealed class JournalMeasurementHost : IAsyncDisposable
{
    private readonly string _dataDir;
    private readonly Ledger _ledger;

    private JournalMeasurementHost(string dataDir, Ledger ledger, JournalMeasuringSegmentWriter writer, JournalCoordinator journal, DurableMutationExecutor executor)
    {
        _dataDir = dataDir;
        _ledger = ledger;
        Writer = writer;
        Journal = journal;
        Executor = executor;
    }

    /// <summary>Gets the executor over <see cref="Journal" />.</summary>
    internal DurableMutationExecutor Executor { get; }

    /// <summary>Gets the production journal coordinator.</summary>
    internal JournalCoordinator Journal { get; }

    /// <summary>Gets the measuring and stallable writer injected into <see cref="Journal" />.</summary>
    internal JournalMeasuringSegmentWriter Writer { get; }

    /// <summary>Releases any stall, stops the journal and deletes the data directory.</summary>
    /// <returns>An asynchronous operation.</returns>
    public async ValueTask DisposeAsync()
    {
        Writer.Release();
        await Journal.DisposeAsync().ConfigureAwait(false);
        _ledger.Dispose();
        try
        {
            Directory.Delete(_dataDir, true);
        }
        catch (IOException)
        {
            // Best effort cleanup of a scratch directory.
        }
    }

    /// <summary>Creates the host in a fresh directory under <paramref name="root" />.</summary>
    /// <param name="root">Directory on the disk under test.</param>
    /// <param name="groupCommit">Whether journal group commit is enabled (1 ms wait, batch of 32, as in the group-commit benchmark).</param>
    /// <returns>The started host.</returns>
    internal static async Task<JournalMeasurementHost> CreateAsync(string root, bool groupCommit)
    {
        ArgumentException.ThrowIfNullOrEmpty(root);
        var dataDir = Path.Join(root, "squirix-journal-measure-" + Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(dataDir);
        var options = new PersistenceOptions
        {
            DataDir = dataDir,
            JournalGroupCommitMaxWait = groupCommit ? TimeSpan.FromMilliseconds(1) : TimeSpan.Zero,
            JournalGroupCommitMaxBatch = 32,
            JournalMaxSegmentMb = 64,
            JournalMaxTotalBytesMb = 32768,
            JournalMaxSegmentCount = 1024,
        };
        var ledger = new Ledger(options, NullLogger<Ledger>.Instance);
        var writer = new JournalMeasuringSegmentWriter();
        var manifest = await ledger.ReadCurrentOrDefaultAsync(CancellationToken.None).ConfigureAwait(false);
        var journal = new JournalCoordinator(options, manifest, ledger, new AsyncManualResetEvent(true), writer, NullLoggerFactory.Instance, TimeProvider.System);
        return new JournalMeasurementHost(dataDir, ledger, writer, journal, new DurableMutationExecutor(journal, NullLogger<DurableMutationExecutor>.Instance));
    }
}
