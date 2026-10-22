using System;
using System.Globalization;
using System.Runtime.InteropServices;
using Squirix.Attributes;

namespace Squirix.ProtocolModel;

[Immutable]
internal sealed class ExploreProfile
{
    private ExploreProfile(string name, in ExploreBounds bounds, ExploreFlags flags)
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
        AllowDiskLoss = flags.AllowDiskLoss;
        LogRules = flags.LogRules;
    }

    /// <summary>Gets a value indicating whether one node may lose its disk and a leader may repair it.</summary>
    internal bool AllowDiskLoss { get; }

    internal bool AllowCrash { get; }

    internal bool AllowPartition { get; }

    /// <summary>
    /// Gets a value indicating whether the profile is about the log and the commit rule: an election is one step, a leader resends the next
    /// entry a member needs, and the applied index stays where it is.
    /// </summary>
    /// <remarks>
    /// One-step elections and no applied index shorten the schedules enough to reach four terms; the vote exchange and the reads are
    /// explored by the other profiles. The resend is what lets an entry of an earlier term reach a majority in a later term.
    /// </remarks>
    internal bool LogRules { get; }

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
        var isSmall = string.Equals(profile, "small", StringComparison.OrdinalIgnoreCase);
        var isFull = string.Equals(profile, "full", StringComparison.OrdinalIgnoreCase);
        return (isSmall, isFull) switch
        {
            (true, _) => Small(symmetryReduce),
            (false, true) => Full(symmetryReduce),
            (false, false) => throw new ArgumentOutOfRangeException(nameof(profile), profile, "Expected small or full."),
        };
    }

    internal static ExploreProfile ForReplicaCount(int replicaCount, int maxTerm, int maxLogEntries, int maxInFlight, int maxPendingReads, bool allowCrash, bool symmetryReduce)
    {
        // Vote/read ack bits are packed in Int32 masks (node id 0..31).
        if (replicaCount is < 1 or > 32)
            throw new ArgumentOutOfRangeException(nameof(replicaCount), replicaCount, "Replica count must be between 1 and 32.");

        var bounds = new ExploreBounds(replicaCount, maxTerm, maxLogEntries, maxInFlight, maxPendingReads, 50_000);
        var flags = new ExploreFlags(allowCrash, replicaCount >= 3 && allowCrash, symmetryReduce, false, false);
        return new ExploreProfile("rf-" + replicaCount.ToString(CultureInfo.InvariantCulture), in bounds, flags);
    }

    /// <summary>Builds a profile in which one node may lose its disk and a leader may repair it, with crashes and without partitions.</summary>
    /// <param name="replicaCount">The number of replicas.</param>
    /// <param name="maxTerm">The highest term explored.</param>
    /// <param name="maxInFlight">The most messages in flight.</param>
    /// <param name="maxStates">The state budget.</param>
    /// <returns>The profile.</returns>
    /// <remarks>Message loss stands in for partitions: a dropped message isolates its receiver for that message.</remarks>
    internal static ExploreProfile ForDiskLoss(int replicaCount, int maxTerm, int maxInFlight, int maxStates)
    {
        var bounds = new ExploreBounds(replicaCount, maxTerm, 1, maxInFlight, 0, maxStates);
        return new ExploreProfile("disk-loss-rf-" + replicaCount.ToString(CultureInfo.InvariantCulture), in bounds, new ExploreFlags(true, false, true, true, false));
    }

    /// <summary>Builds the small disk-loss profile: three replicas, two terms, one entry.</summary>
    /// <returns>The profile.</returns>
    internal static ExploreProfile SmallDiskLoss() => ForDiskLoss(3, 2, 2, 300_000);

    /// <summary>Builds the small log-rules profile: three terms, two entries, one message in flight; it reaches its fixed point.</summary>
    /// <returns>The profile.</returns>
    internal static ExploreProfile SmallLogRules() => ForLogRules(3, 2, 1, 300_000);

    /// <summary>Builds the log-rules profile that reaches the loss of an earlier-term entry: four terms, two entries, one message in flight.</summary>
    /// <returns>The profile.</returns>
    /// <remarks>Its fixed point is about one million states; the unit tests stop at the first violation of a broken rule, the full profile explores it.</remarks>
    internal static ExploreProfile EarlierTermLogRules() => ForLogRules(4, 2, 1, 1_500_000);

    /// <summary>Builds a profile for the log rules: three replicas, crashes, no partitions, no reads.</summary>
    /// <param name="maxTerm">The highest term explored.</param>
    /// <param name="maxLogEntries">The longest log explored.</param>
    /// <param name="maxInFlight">The most messages in flight.</param>
    /// <param name="maxStates">The state budget.</param>
    /// <returns>The profile.</returns>
    /// <remarks>An earlier-term entry is lost to a later leader only after four elections, so the commit rule needs four terms.</remarks>
    internal static ExploreProfile ForLogRules(int maxTerm, int maxLogEntries, int maxInFlight, int maxStates)
    {
        var bounds = new ExploreBounds(3, maxTerm, maxLogEntries, maxInFlight, 0, maxStates);
        return new ExploreProfile("log-rules-rf-3-terms-" + maxTerm.ToString(CultureInfo.InvariantCulture), in bounds, new ExploreFlags(true, false, true, false, true));
    }

    /// <summary>Builds the profile a production trace is matched in: the small commit bounds with the terms the trace uses and one read when it has one.</summary>
    /// <param name="maxTerm">The highest term in the trace.</param>
    /// <param name="reads">Whether the trace observes a read.</param>
    /// <returns>The profile, without symmetry reduction.</returns>
    /// <remarks>A smaller model can only reject more, so a trace outside these bounds fails loudly instead of being accepted.</remarks>
    internal static ExploreProfile ForTrace(int maxTerm, bool reads) => new(
        "trace",
        new ExploreBounds(3, maxTerm, 1, 2, reads ? 1 : 0, reads ? 300_000 : 50_000),
        new ExploreFlags(false, false, false, false, false));

    internal static ExploreProfile SmallCommit(bool symmetryReduce = true) => SmallProfile("small-commit", 0, symmetryReduce);

    internal static ExploreProfile SmallElection(bool symmetryReduce = true) => SmallProfile("small-election", 0, symmetryReduce);

    internal static ExploreProfile SmallRead(bool symmetryReduce = true) => SmallProfile("small-read", 1, symmetryReduce, 300_000);

    private static ExploreProfile Full(bool symmetryReduce = true) => new(
        "full",
        new ExploreBounds(3, 3, 3, 4, 1, 50_000),
        new ExploreFlags(true, true, symmetryReduce, false, false));

    private static ExploreProfile Small(bool symmetryReduce = true) => SmallProfile("small", 0, symmetryReduce);

    private static ExploreProfile SmallProfile(string name, int maxPendingReads, bool symmetryReduce, int maxStates = 50_000) => new(
        name,
        new ExploreBounds(3, 2, 1, 2, maxPendingReads, maxStates),
        new ExploreFlags(false, false, symmetryReduce, false, false));

    [StructLayout(LayoutKind.Auto)]
    [Immutable]
    private readonly record struct ExploreBounds(int ReplicaCount, int MaxTerm, int MaxLogEntries, int MaxInFlight, int MaxPendingReads, int MaxStates);

    [StructLayout(LayoutKind.Auto)]
    [Immutable]
    private readonly record struct ExploreFlags(bool AllowCrash, bool AllowPartition, bool SymmetryReduce, bool AllowDiskLoss, bool LogRules);
}
