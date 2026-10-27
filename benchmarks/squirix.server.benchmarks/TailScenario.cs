namespace Squirix.Server.Benchmarks;

/// <summary>One cell of the flush tail measurement matrix.</summary>
/// <param name="GroupCommit">Whether journal group commit is enabled.</param>
/// <param name="Writers">The number of concurrent writers.</param>
/// <param name="PayloadBytes">The PUT payload size in bytes.</param>
/// <param name="NoiseWriters">The number of noisy neighbour writers on the same volume; zero is a quiet disk.</param>
internal sealed record TailScenario(bool GroupCommit, int Writers, int PayloadBytes, int NoiseWriters);
