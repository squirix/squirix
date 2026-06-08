using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Benchmarks.Infrastructure;
using Squirix.Server.TestKit.Limits;

namespace Squirix.Benchmarks;

/// <summary>
/// End-to-end insert benchmarks comparing small vs near-limit payloads through the full client write path.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
[SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "BenchmarkDotNet prefers instance members.")]
public class EntryPayloadInsertBenchmarks : RemoteBenchmarkLifecycleBase
{
    private const int BatchSize = 32;

    private string _largeValue = null!;
    private string _smallValue = null!;

    /// <summary>
    /// Starts the node and prepares payload strings.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        StartNode();
        StartSharedCache("bench-entry-payload");
        _smallValue = new string('x', 256);
        _largeValue = EntryPayloadLimitTestHelpers.CreateNearLimitDiscriminatedStringValue();
    }

    /// <summary>
    /// Stops the shared cache session and benchmark node.
    /// </summary>
    [GlobalCleanup]
    public void Cleanup()
    {
        StopSharedCache();
        StopNode();
    }

    /// <summary>
    /// Inserts small string values through the public client SDK.
    /// </summary>
    /// <returns>A task that completes when the batch finishes.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize, Description = "SetAsync small payload")]
    public async Task InsertSmallPayloadBatched()
    {
        for (var i = 0; i < BatchSize; i++)
            await SharedCache.SetAsync(Guid.NewGuid().ToString("N"), _smallValue, cancellationToken: CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Inserts near-limit string values through the public client SDK.
    /// </summary>
    /// <returns>A task that completes when the batch finishes.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize, Description = "SetAsync near-limit payload")]
    public async Task InsertNearLimitPayloadBatched()
    {
        for (var i = 0; i < BatchSize; i++)
            await SharedCache.SetAsync(Guid.NewGuid().ToString("N"), _largeValue, cancellationToken: CancellationToken.None).ConfigureAwait(false);
    }
}
