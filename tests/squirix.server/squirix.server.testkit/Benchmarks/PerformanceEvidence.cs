namespace Squirix.Server.TestKit.Benchmarks;

/// <summary>Stored performance evidence for one benchmark phase.</summary>
/// <param name="BenchmarkName">Benchmark full type name.</param>
/// <param name="Phase">Evidence phase name.</param>
/// <param name="Machine">Machine fingerprint captured at measurement time.</param>
/// <param name="Baseline">Baseline value (lower is better, for example nanoseconds per operation).</param>
/// <param name="Actual">Measured value.</param>
/// <param name="AllowedRegressionPercent">Allowed regression in percent before the gate fails.</param>
public sealed record PerformanceEvidence(string BenchmarkName, string Phase, string Machine, double Baseline, double Actual, double AllowedRegressionPercent);
