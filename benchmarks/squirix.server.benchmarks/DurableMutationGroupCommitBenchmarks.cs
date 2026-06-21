using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.App;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit.Benchmarks;

namespace Squirix.Server.Benchmarks;

/// <summary>Compares durable mutation group-commit throughput via <see cref="DurableMutationExecutor" />.</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3)]
public class DurableMutationGroupCommitBenchmarks
{
    private const int DefaultOperationsPerWriter = 2_000;
    private const int DefaultParallelWriters = 8;
    private int _nextWriterId;
    private DurableMutationExecutor? _executor;
    private JournalBenchmarkHost? _host;
    private byte[] _putPayload = [];

    /// <summary>Gets or sets the journal backend under test.</summary>
    [Params(JournalBackend.JsonFramed, JournalBackend.Pipelined)]
    public JournalBackend Backend { get; set; }

    /// <summary>Gets or sets the PUT payload size in bytes.</summary>
    [Params(256, 4096)]
    public int PutPayloadBytes { get; set; }

    /// <summary>Creates the journal coordinator, executor, and payload for the current parameter set.</summary>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        var options = new PersistenceOptions
        {
            JournalBackend = Backend,
            JournalPlatformBackend = JournalPlatformBackend.RandomAccess,
            JournalGroupCommitMaxWait = TimeSpan.FromMilliseconds(1),
            JournalGroupCommitMaxBatch = 32,
            JournalMaxSegmentMb = 64,
        };
        _host = await JournalBenchmarkHost.CreateAsync("durable-mutation-gc-bench", options, CancellationToken.None).ConfigureAwait(false);
        _executor = new DurableMutationExecutor(_host.Coordinator);
        _putPayload = new byte[PutPayloadBytes];
        Array.Fill(_putPayload, Convert.ToByte('m'));
        _nextWriterId = 0;
    }

    /// <summary>Disposes the journal coordinator and temporary data directory.</summary>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        _executor = null;
        if (_host is not null)
            await _host.DisposeAsync().ConfigureAwait(false);
        _host = null;
    }

    /// <summary>Runs durable PUT mutations with per-key group commit (production-like path).</summary>
    [Benchmark]
    public async Task ExecutePutMutationAsync()
    {
        var host = _host ?? throw new InvalidOperationException("Benchmark host was not initialized.");
        var executor = _executor ?? throw new InvalidOperationException("Benchmark executor was not initialized.");
        var payload = _putPayload;
        var operationsPerWriter = GetOperationsPerWriter();
        var parallelWriters = GetParallelWriters();
        await Parallel.ForEachAsync(
            new int[parallelWriters],
            new ParallelOptions { MaxDegreeOfParallelism = parallelWriters },
            async (_, cancellationToken) =>
            {
                var writerId = Interlocked.Increment(ref _nextWriterId);
                var key = new CacheKey("bench", $"m{writerId.ToString(System.Globalization.CultureInfo.InvariantCulture)}");
                for (var i = 0; i < operationsPerWriter; i++)
                {
                    await executor.ExecuteAsync(
                        key,
                        static _ => ValueTask.FromResult(DurableMutationCondition<int>.Apply()),
                        ct => host.Coordinator.AppendPutAsync(key, payload, null, ct),
                        static _ => new ValueTask<int>(1),
                        cancellationToken).ConfigureAwait(false);
                }
            }).ConfigureAwait(false);
    }

    private static int GetOperationsPerWriter() =>
        JournalAppendBreakdownBenchmarkSupport.ResolveGroupCommitOperationsPerWriter(DefaultOperationsPerWriter);

    private static int GetParallelWriters() =>
        JournalAppendBreakdownBenchmarkSupport.ResolveGroupCommitParallelWriters(DefaultParallelWriters);
}
