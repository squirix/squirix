using System.Collections.Generic;

namespace Squirix.ProtocolModel;

internal sealed class ExploreResult
{
    internal ExploreResult(
        int statesVisited,
        int transitionsApplied,
        SafetyViolation? violation,
        ClusterState? violatingState,
        bool fixedPointReached,
        IReadOnlyList<string>? counterexamplePath)
    {
        StatesVisited = statesVisited;
        TransitionsApplied = transitionsApplied;
        Violation = violation;
        ViolatingState = violatingState;
        FixedPointReached = fixedPointReached;
        CounterexamplePaths = counterexamplePath;
    }

    internal IReadOnlyList<string>? CounterexamplePaths { get; }

    internal bool FixedPointReached { get; }

    internal int StatesVisited { get; }

    internal int TransitionsApplied { get; }

    internal ClusterState? ViolatingState { get; }

    internal SafetyViolation? Violation { get; }
}
