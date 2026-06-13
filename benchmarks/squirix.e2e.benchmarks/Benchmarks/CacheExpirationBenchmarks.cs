using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Squirix.E2EBenchmarks.Benchmarks;

/// <summary>
/// End-to-end public API benchmarks for expiration operations.
/// </summary>
[BenchmarkCategory("e2e", "expiration", "mutation")]
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "BenchmarkDotNet discovers benchmark classes by public type.")]
[SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "BenchmarkDotNet requires instance benchmark members.")]
public class CacheExpirationBenchmarks : CacheBenchmarkBase
{
    private const int DestructiveExpirationBatchSize = 512;
    private static readonly TimeSpan LongExpiration = TimeSpan.FromHours(1);
    private int _removeExpirationOffset;

    /// <summary>
    /// Measures GetExpirationAsync for an expiring entry.
    /// </summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    [BenchmarkCategory("expiration", "read")]
    public async Task GetExpirationShouldReturnExpiringEntry()
    {
        for (var i = 0; i < BatchSize; i++)
            Consumer.Consume(await Adapter.GetExpirationAsync(NextExpiringHitKey(), CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>
    /// Measures GetExpirationAsync for a non-expiring entry.
    /// </summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    [BenchmarkCategory("expiration", "read")]
    public async Task GetExpirationShouldReturnNonExpiringEntry()
    {
        for (var i = 0; i < BatchSize; i++)
            Consumer.Consume(await Adapter.GetExpirationAsync(NextHitKey(), CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>
    /// Measures RemoveExpirationAsync for pre-seeded expiring entries.
    /// </summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = DestructiveExpirationBatchSize)]
    [BenchmarkCategory("expiration", "mutation")]
    public async Task RemoveExpirationShouldClearExpiration()
    {
        for (var i = 0; i < DestructiveExpirationBatchSize; i++)
            Consumer.Consume(await Adapter.RemoveExpirationAsync(Keyspace.ExpiringHitKey(i), CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>
    /// Re-seeds expiring entries outside the measured body for destructive RemoveExpirationAsync benchmarks.
    /// </summary>
    [IterationSetup(Target = nameof(RemoveExpirationShouldClearExpiration))]
    public void SeedRemoveExpirationIteration()
    {
        var offset = Interlocked.Add(ref _removeExpirationOffset, DestructiveExpirationBatchSize);
        for (var i = 0; i < DestructiveExpirationBatchSize; i++)
            Adapter.SetExpiringAsync(Keyspace.ExpiringHitKey(i), offset + i, LongExpiration, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <summary>
    /// Measures TouchAsync absolute expiration path.
    /// </summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    [BenchmarkCategory("expiration", "mutation")]
    public async Task TouchShouldUpdateAbsoluteExpiration()
    {
        var expiresAt = DateTimeOffset.UtcNow.AddHours(1);
        for (var i = 0; i < BatchSize; i++)
            Consumer.Consume(await Adapter.TouchAbsoluteAsync(NextHitKey(), expiresAt, CancellationToken.None).ConfigureAwait(false));
    }

    /// <summary>
    /// Measures TouchAsync relative expiration path.
    /// </summary>
    /// <returns>A task that completes when the batch has finished.</returns>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    [BenchmarkCategory("expiration", "mutation")]
    public async Task TouchShouldUpdateRelativeExpiration()
    {
        for (var i = 0; i < BatchSize; i++)
            Consumer.Consume(await Adapter.TouchRelativeAsync(NextHitKey(), LongExpiration, CancellationToken.None).ConfigureAwait(false));
    }

    /// <inheritdoc />
    protected override Task SeedAdditionalStateAsync(CancellationToken cancellationToken) => Adapter.SeedExpiringAsync(Keyspace.ExpiringHitKeys, LongExpiration, cancellationToken);
}
