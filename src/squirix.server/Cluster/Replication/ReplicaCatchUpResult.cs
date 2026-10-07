using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>What an entry catch-up session achieved before it ended.</summary>
/// <param name="Outcome">How the session ended.</param>
/// <param name="HeldThrough">The highest log index the follower is verified to hold after the session; zero when nothing was verified.</param>
/// <param name="HeldTerm">The term of the entry at <paramref name="HeldThrough" />; zero at the log origin.</param>
/// <param name="FollowerTerm">The follower's term when it refused the leader's as stale; zero otherwise.</param>
/// <param name="EntriesSent">The number of entries the follower accepted from the session.</param>
/// <param name="Rounds">The number of append requests the session sent.</param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct ReplicaCatchUpResult(
    ReplicaCatchUpOutcome Outcome,
    ulong HeldThrough,
    ulong HeldTerm,
    ulong FollowerTerm,
    int EntriesSent,
    int Rounds);
