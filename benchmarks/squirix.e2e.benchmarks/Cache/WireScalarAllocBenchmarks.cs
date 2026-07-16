using System.Diagnostics.CodeAnalysis;
using BenchmarkDotNet.Attributes;
using Squirix.E2EBenchmarks.Support.Harness;

namespace Squirix.E2EBenchmarks.Cache;

/// <summary>
/// End-to-end allocation baselines for scalar cache values on the gRPC wire path.
/// Re-run with the same filter after changing wire encoding to compare allocated bytes.
/// </summary>
public class WireScalarAllocBenchmarks : WireAllocBenchmarkBase<string>
{
    /// <summary>Re-seeds hit keys outside the measured remove benchmark body.</summary>
    [IterationSetup(Target = nameof(RemoveAsync))]
    [SuppressMessage("Usage", "VSTHRD002:Avoid problematic synchronous waits", Justification = "BenchmarkDotNet IterationSetup must return void on concrete benchmark types.")]
    public void SeedRemoveIteration() => SeedRemoveIterationCoreAsync().GetAwaiter().GetResult();

    /// <summary>Re-seeds expiring entries outside the measured remove-expiration benchmark body.</summary>
    [IterationSetup(Target = nameof(RemoveExpirationAsync))]
    [SuppressMessage("Usage", "VSTHRD002:Avoid problematic synchronous waits", Justification = "BenchmarkDotNet IterationSetup must return void on concrete benchmark types.")]
    public void SeedRemoveExpirationIteration() => SeedRemoveExpirationIterationCoreAsync().GetAwaiter().GetResult();

    /// <inheritdoc />
    protected override string GetCacheName() => "bench-wire-scalar-alloc";

    /// <inheritdoc />
    protected override string CreateValue(int index) => E2EBenchmarkDataFactory.CreateSmallString(index);

    /// <inheritdoc />
    protected override void ConsumeValue(string? value) => Consumer.Consume(value ?? string.Empty);
}
