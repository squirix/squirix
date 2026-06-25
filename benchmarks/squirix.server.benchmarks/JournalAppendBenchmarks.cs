using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Benchmarks;

/// <summary>Pipelined journal append throughput benchmarks.</summary>
[SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global", Justification = "BenchmarkDotNet [Params] properties require public setters.")]
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public sealed class JournalAppendBenchmarks
{
    private const int OperationsPerInvoke = 100_000;
    private JournalBenchmarkHost? _host;
    private CacheKey _key = new("bench", "key");
    private byte[] _putPayload = [];

    /// <summary>Gets or sets the group commit wait values.</summary>
    [ParamsSource(nameof(GroupCommitMaxWaitValues))]
    public TimeSpan GroupCommitMaxWait { get; set; }

    /// <summary>Gets or sets the platform segment writer for pipelined journal.</summary>
    [Params(JournalPlatformBackend.RandomAccess)]
    public JournalPlatformBackend PlatformBackend { get; set; }

    /// <summary>Gets or sets the PUT payload size in bytes.</summary>
    [Params(256, 4096)]
    public int PutPayloadBytes { get; set; }

    /// <summary>Gets or sets group-commit max wait (zero disables batching delay).</summary>
    public static IEnumerable<TimeSpan> GroupCommitMaxWaitValues()
    {
        yield return TimeSpan.Zero;
        yield return TimeSpan.FromMilliseconds(1);
    }

    /// <summary>Appends PUT operations and awaits durability after each append.</summary>
    /// <returns>A task that completes when all operations finish.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark host was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task AppendPutAsync()
    {
        var host = _host ?? throw new InvalidOperationException("Benchmark host was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
        {
            await host.Coordinator.AppendPutAsync(_key, _putPayload, null, CancellationToken.None).ConfigureAwait(false);
            await host.Coordinator.AwaitDurabilityCommitAsync(CancellationToken.None).ConfigureAwait(false);
        }
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

    /// <summary>Creates the journal coordinator and payload for the current parameter set.</summary>
    /// <returns>A task that completes when setup finishes.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        var options = new PersistenceOptions
        {
            JournalPlatformBackend = PlatformBackend,
            JournalGroupCommitMaxWait = GroupCommitMaxWait,
            JournalGroupCommitMaxBatch = 32,
            JournalMaxSegmentMb = 64,
        };
        _host = await JournalBenchmarkHost.CreateAsync("journal-bench", options, CancellationToken.None).ConfigureAwait(false);
        _putPayload = new byte[PutPayloadBytes];
        Array.Fill(_putPayload, Convert.ToByte('x'));
        _key = new CacheKey("bench", $"payload-{PutPayloadBytes}");
    }
}
