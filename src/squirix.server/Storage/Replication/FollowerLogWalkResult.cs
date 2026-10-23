using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>Result of walking the log frames during startup recovery.</summary>
/// <param name="LastValidEnd">The byte offset after the last valid frame.</param>
/// <param name="Truncated">Determines whether a divergent tail was truncated.</param>
/// <param name="BoundaryDivergent">
/// Determines whether the surviving journal diverges from the restored
/// snapshot at the snapshot's included index.
/// </param>
[Immutable]
internal readonly record struct FollowerLogWalkResult(long LastValidEnd, bool Truncated, bool BoundaryDivergent = false);
