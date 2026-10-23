namespace Squirix.E2ETests.Fixtures;

/// <summary>The election timing a series ran with.</summary>
/// <param name="ElectionTimeoutMs">The time without leader contact after which a follower starts an election.</param>
/// <param name="HeartbeatMs">The time between two heartbeats of a leader.</param>
/// <param name="MaxJitterMs">The largest random delay added to the election timeout.</param>
/// <param name="VoteRpcTimeoutMs">The longest wait for one vote reply.</param>
/// <param name="JitterSeed">The seed of the jitter, which the testkit mixes with each node identifier.</param>
internal sealed record ElectionTimingEvidence(double ElectionTimeoutMs, double HeartbeatMs, double MaxJitterMs, double VoteRpcTimeoutMs, ulong JitterSeed);
