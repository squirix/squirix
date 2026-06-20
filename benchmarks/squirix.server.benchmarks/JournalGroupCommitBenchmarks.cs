using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit.IO;

namespace Squirix.Server.Benchmarks;

/// <summary>Compares JsonFramed vs Pipelined under concurrent durable writers with group commit enabled.</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3)]
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "BenchmarkDotNet discovers benchmark classes by public type.")]
public class JournalGroupCommitBenchmarks
{
    private const int OperationsPerWriter = 2_000;
    private const int ParallelWriters = 8;
    private int _nextWriterId;
    private JournalBenchmarkHost? _host;
    private byte[] _putPayload = [];

    /// <summary>Gets or sets the journal backend under test.</summary>
    [Params(JournalBackend.JsonFramed, JournalBackend.Pipelined)]
    [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global", Justification = "Property annotated with [Params] must have a public setter")]
    public JournalBackend Backend { get; set; }

    /// <summary>Gets or sets the PUT payload size in bytes.</summary>
    [Params(256, 4096)]
    [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global", Justification = "Property annotated with [Params] must have a public setter")]
    public int PutPayloadBytes { get; set; }

    /// <summary>Creates the journal coordinator and payload for the current parameter set.</summary>
    /// <returns>A task that completes when setup finishes.</returns>
    [GlobalSetup]
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Ownership transfers to _host disposed in GlobalCleanup.")]
    public async Task SetupAsync()
    {
        var dir = DirectoryKit.CreateTempDirectory("journal-gc-bench");
        var options = new PersistenceOptions
        {
            DataDir = dir,
            JournalBackend = Backend,
            JournalPlatformBackend = JournalPlatformBackend.RandomAccess,
            JournalGroupCommitMaxWait = TimeSpan.FromMilliseconds(1),
            JournalGroupCommitMaxBatch = 32,
            JournalMaxSegmentMb = 64,
        };
        _host = await JournalBenchmarkHost.CreateAsync(options, CancellationToken.None).ConfigureAwait(false);
        _putPayload = new byte[PutPayloadBytes];
        Array.Fill(_putPayload, Convert.ToByte('a'));
        _nextWriterId = 0;
    }

    /// <summary>Disposes the journal coordinator created during setup.</summary>
    /// <returns>A task that completes when cleanup finishes.</returns>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        if (_host is not null)
            await _host.DisposeAsync().ConfigureAwait(false);
        _host = null;
    }

    /// <summary>Appends PUT operations from parallel writers and awaits durability after each append.</summary>
    /// <returns>A task that completes when all operations finish.</returns>
    [Benchmark(OperationsPerInvoke = OperationsPerWriter * ParallelWriters)]
    public async Task ConcurrentAppendPutAsync()
    {
        var host = _host ?? throw new InvalidOperationException("Benchmark host was not initialized.");
        await Parallel.ForEachAsync(
            new int[ParallelWriters],
            new ParallelOptions { MaxDegreeOfParallelism = ParallelWriters },
            async (_, cancellationToken) =>
            {
                var writerId = Interlocked.Increment(ref _nextWriterId);
                var key = new CacheKey("bench", $"w{writerId.ToString(System.Globalization.CultureInfo.InvariantCulture)}");
                for (var i = 0; i < OperationsPerWriter; i++)
                {
                    await host.Coordinator.AppendPutAsync(key, _putPayload, null, cancellationToken).ConfigureAwait(false);
                    await host.Coordinator.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
                }
            }).ConfigureAwait(false);
    }
}
