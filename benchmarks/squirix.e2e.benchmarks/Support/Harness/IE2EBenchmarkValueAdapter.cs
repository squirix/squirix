namespace Squirix.E2EBenchmarks.Support.Harness;

/// <summary>Non-generic adapter over typed cache operations for a benchmark value shape.</summary>
internal interface IE2EBenchmarkValueAdapter : IE2EBenchmarkValueReads, IE2EBenchmarkValueMutations, IE2EBenchmarkValueSeeding;
