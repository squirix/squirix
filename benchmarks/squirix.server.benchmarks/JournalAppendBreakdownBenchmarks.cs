using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit.Benchmarks;

namespace Squirix.Server.Benchmarks;

/// <summary>Isolates encode, enqueue, fsync, and combined append+durability costs for the pipelined journal.</summary>
[SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global", Justification = "BenchmarkDotNet [Params] properties require public setters.")]
[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3)]
public class JournalAppendBreakdownBenchmarks
{
    private const int DefaultOperationsPerInvoke = 100_000;
    private JournalBenchmarkHost? _host;
    private CacheKey _key = new("bench", "breakdown");
    private byte[] _putPayload = [];

    /// <summary>Gets or sets the PUT payload size in bytes.</summary>
    [Params(256, 4096)]
    public int PutPayloadBytes { get; set; }

    /// <summary>Combined append and durability using the coordinator fast path when available.</summary>
    [Benchmark(Baseline = true, OperationsPerInvoke = DefaultOperationsPerInvoke)]
    public async Task AppendPutWithDurabilityAsync()
    {
        var host = _host ?? throw new InvalidOperationException("Benchmark host was not initialized.");
        var operations = GetOperationsPerInvoke();
        for (var i = 0; i < operations; i++)
            await host.Coordinator.AppendPutAndAwaitDurabilityAsync(_key, _putPayload, null, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Disposes the journal coordinator created during setup.</summary>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        if (_host is not null)
            await _host.DisposeAsync().ConfigureAwait(false);
        _host = null;
    }

    /// <summary>Encodes one journal frame without touching the coordinator or disk.</summary>
    [Benchmark(OperationsPerInvoke = DefaultOperationsPerInvoke)]
    public void EncodeOnly()
    {
        var operations = GetOperationsPerInvoke();
        for (var i = 0; i < operations; i++)
        {
            var frameLen = JournalAppendBreakdownBenchmarkSupport.EncodePipelinedPutFrame(_key.Namespace, _key.Key, _putPayload, out var rented);
            ArrayPool<byte>.Shared.Return(rented);
            GC.KeepAlive(frameLen);
        }
    }

    /// <summary>Appends without awaiting durability, measuring ring / writer enqueue only.</summary>
    [Benchmark(OperationsPerInvoke = DefaultOperationsPerInvoke)]
    public async Task EnqueueOnlyAsync()
    {
        var host = _host ?? throw new InvalidOperationException("Benchmark host was not initialized.");
        var operations = GetOperationsPerInvoke();
        for (var i = 0; i < operations; i++)
            await host.Coordinator.AppendPutAsync(_key, _putPayload, null, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Awaits durability only after prefilled dirty journal state.</summary>
    [Benchmark(OperationsPerInvoke = DefaultOperationsPerInvoke)]
    public async Task FsyncOnlyAsync()
    {
        var host = _host ?? throw new InvalidOperationException("Benchmark host was not initialized.");
        var operations = GetOperationsPerInvoke();
        for (var i = 0; i < operations; i++)
        {
            await host.Coordinator.AppendPutAsync(_key, _putPayload, null, CancellationToken.None).ConfigureAwait(false);
            await host.Coordinator.AwaitDurabilityCommitAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }

    /// <summary>Creates the journal coordinator and payload for the current parameter set.</summary>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        var options = new PersistenceOptions
        {
            JournalPlatformBackend = JournalPlatformBackend.RandomAccess,
            JournalGroupCommitMaxWait = TimeSpan.Zero,
            JournalGroupCommitMaxBatch = 32,
            JournalMaxSegmentMb = 64,
        };
        _host = await JournalBenchmarkHost.CreateAsync("journal-breakdown-bench", options, CancellationToken.None).ConfigureAwait(false);
        _putPayload = new byte[PutPayloadBytes];
        Array.Fill(_putPayload, Convert.ToByte('b'));
        _key = new CacheKey("bench", $"breakdown-{PutPayloadBytes.ToString(CultureInfo.InvariantCulture)}");
    }

    private static int GetOperationsPerInvoke() => JournalAppendBreakdownBenchmarkSupport.ResolveOperationsPerInvoke(DefaultOperationsPerInvoke);
}
