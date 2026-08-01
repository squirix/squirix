using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.E2EBenchmarks.Fixtures;
using Squirix.E2EBenchmarks.Support.Harness;

namespace Squirix.E2EBenchmarks.Cache;

/// <summary>
/// End-to-end allocation baselines for structured cache values on the gRPC wire path.
/// Re-run with the same filter after changing wire encoding to compare allocated bytes.
/// </summary>
public class WireStructuredAllocBenchmarks : WireAllocBenchmarkBase<BenchmarkUserProfile>
{
    /// <summary>Re-seeds expiring entries outside the measured remove-expiration benchmark body.</summary>
    /// <returns>A task that completes when reseeding finishes.</returns>
    [IterationSetup(Target = nameof(RemoveExpirationAsync))]
    public Task SeedRemoveExpirationIterationAsync() => SeedRemoveExpirationIterationCoreAsync();

    /// <summary>Re-seeds hit keys outside the measured remove benchmark body.</summary>
    /// <returns>A task that completes when reseeding finishes.</returns>
    [IterationSetup(Target = nameof(RemoveAsync))]
    public Task SeedRemoveIterationAsync() => SeedRemoveIterationCoreAsync();

    /// <inheritdoc />
    protected override void ConsumeValue(BenchmarkUserProfile? value) => Consumer.Consume(value?.Id ?? 0);

    /// <inheritdoc />
    protected override BenchmarkUserProfile CreateValue(int index) => E2EBenchmarkDataFactory.CreateUserProfile(index);

    /// <inheritdoc />
    protected override string GetCacheName() => "bench-wire-structured-alloc";
}
