using System;
using System.Globalization;
using System.Runtime.InteropServices;

namespace Squirix.ProtocolModel;

internal sealed class ExploreProfile
{
    private ExploreProfile(string name, ExploreBounds bounds, ExploreFlags flags)
    {
        Name = name;
        ReplicaCount = bounds.ReplicaCount;
        MaxTerm = bounds.MaxTerm;
        MaxLogEntries = bounds.MaxLogEntries;
        MaxInFlight = bounds.MaxInFlight;
        MaxPendingReads = bounds.MaxPendingReads;
        MaxStates = bounds.MaxStates;
        AllowCrash = flags.AllowCrash;
        AllowPartition = flags.AllowPartition;
        SymmetryReduce = flags.SymmetryReduce;
    }

    internal bool AllowCrash { get; }

    internal bool AllowPartition { get; }

    internal int Majority => (ReplicaCount / 2) + 1;

    internal int MaxInFlight { get; }

    internal int MaxLogEntries { get; }

    internal int MaxPendingReads { get; }

    internal int MaxStates { get; }

    internal int MaxTerm { get; }

    internal string Name { get; }

    internal int ReplicaCount { get; }

    internal bool SymmetryReduce { get; }

    internal static ExploreProfile ForCli(string profile, bool symmetryReduce)
    {
        if (string.Equals(profile, "small", StringComparison.OrdinalIgnoreCase))
            return Small(symmetryReduce);

        if (string.Equals(profile, "full", StringComparison.OrdinalIgnoreCase))
            return Full(symmetryReduce);

        throw new ArgumentOutOfRangeException(nameof(profile), profile, "Expected small or full.");
    }

    internal static ExploreProfile ForReplicaCount(int replicaCount, int maxTerm, int maxLogEntries, int maxInFlight, int maxPendingReads, bool allowCrash, bool symmetryReduce)
    {
        // Vote/read ack bits are packed in Int32 masks (node id 0..31).
        if (replicaCount is < 1 or > 32)
            throw new ArgumentOutOfRangeException(nameof(replicaCount), replicaCount, "Replica count must be between 1 and 32.");

        var bounds = new ExploreBounds(replicaCount, maxTerm, maxLogEntries, maxInFlight, maxPendingReads, 50_000);
        var flags = new ExploreFlags(allowCrash, replicaCount >= 3 && allowCrash, symmetryReduce);
        return new ExploreProfile("rf-" + replicaCount.ToString(CultureInfo.InvariantCulture), bounds, flags);
    }

    internal static ExploreProfile SmallCommit(bool symmetryReduce = true) => SmallProfile("small-commit", 0, symmetryReduce);

    internal static ExploreProfile SmallElection(bool symmetryReduce = true) => SmallProfile("small-election", 0, symmetryReduce);

    internal static ExploreProfile SmallRead(bool symmetryReduce = true) => SmallProfile("small-read", 1, symmetryReduce, 300_000);

    private static ExploreProfile Full(bool symmetryReduce = true) => new("full", new ExploreBounds(3, 3, 3, 4, 1, 50_000), new ExploreFlags(true, true, symmetryReduce));

    private static ExploreProfile Small(bool symmetryReduce = true) => SmallProfile("small", 0, symmetryReduce);

    private static ExploreProfile SmallProfile(string name, int maxPendingReads, bool symmetryReduce, int maxStates = 50_000) => new(
        name,
        new ExploreBounds(3, 2, 1, 2, maxPendingReads, maxStates),
        new ExploreFlags(false, false, symmetryReduce));

    [StructLayout(LayoutKind.Auto)]
    private readonly record struct ExploreBounds(int ReplicaCount, int MaxTerm, int MaxLogEntries, int MaxInFlight, int MaxPendingReads, int MaxStates);

    [StructLayout(LayoutKind.Auto)]
    private readonly record struct ExploreFlags(bool AllowCrash, bool AllowPartition, bool SymmetryReduce);
}
