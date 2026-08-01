using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.E2EBenchmarks.Support.Harness;

namespace Squirix.E2EBenchmarks.Cache;

/// <summary>
/// End-to-end allocation baselines for scalar cache values on the gRPC wire path.
/// Re-run with the same filter after changing wire encoding to compare allocated bytes.
/// </summary>
public class WireScalarAllocBenchmarks : WireAllocBenchmarkBase<string>
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
    protected override void ConsumeValue(string? value) => Consumer.Consume(value ?? string.Empty);

    /// <inheritdoc />
    protected override string CreateValue(int index) => E2EBenchmarkDataFactory.CreateSmallString(index);

    /// <inheritdoc />
    protected override string GetCacheName() => "bench-wire-scalar-alloc";
}
