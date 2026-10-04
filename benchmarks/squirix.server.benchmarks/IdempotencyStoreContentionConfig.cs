using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Jobs;

namespace Squirix.Server.Benchmarks;

/// <summary>Job for the idempotency store contention benchmarks: one launch with fully optimized code from the first call, because an invocation is one pass too short to wait for tiering.</summary>
public sealed class IdempotencyStoreContentionConfig : ManualConfig
{
    /// <summary>Initializes a new instance of the <see cref="IdempotencyStoreContentionConfig" /> class.</summary>
    public IdempotencyStoreContentionConfig()
    {
        _ = AddJob(
            Job.Default
                .WithStrategy(RunStrategy.Throughput)
                .WithLaunchCount(1)
                .WithWarmupCount(2)
                .WithIterationCount(5)
                .WithInvocationCount(1)
                .WithUnrollFactor(1)
                .WithEnvironmentVariable("DOTNET_TieredCompilation", "0"));
    }
}
