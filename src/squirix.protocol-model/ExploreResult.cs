using System.Collections.Generic;
using Squirix.Attributes;

namespace Squirix.ProtocolModel;

/// <summary>What one exploration of one profile found.</summary>
/// <remarks>
/// A safety violation ends the search: once the data is endangered, the rest of the space is not evidence. A policy
/// violation does not, because it says a step was not the protocol's while the state itself is still worth exploring, so
/// the run keeps going to its fixed point and carries the first policy violation alongside its counts.
/// </remarks>
[Immutable]
internal sealed class ExploreResult
{
    internal ExploreResult(int statesVisited, int transitionsApplied, in ViolationReport violations, bool fixedPointReached, IReadOnlyList<string>? counterexamplePath)
    {
        StatesVisited = statesVisited;
        TransitionsApplied = transitionsApplied;
        Violations = violations;
        FixedPointReached = fixedPointReached;
        CounterexamplePaths = counterexamplePath;
    }

    internal IReadOnlyList<string>? CounterexamplePaths { get; }

    internal bool FixedPointReached { get; }

    internal int StatesVisited { get; }

    internal int TransitionsApplied { get; }

    internal ViolationReport Violations { get; }

    internal ClusterState? ViolatingState => Violations.SafetyState;

    internal SafetyViolation? Violation => Violations.Safety;

    /// <summary>Gets the first step the protocol does not allow, or <see langword="null" /> when every step was allowed.</summary>
    internal PolicyViolation? PolicyViolation => Violations.Policy;

    /// <summary>Gets the state in which that step was taken.</summary>
    internal ClusterState? PolicyViolatingState => Violations.PolicyState;
}
