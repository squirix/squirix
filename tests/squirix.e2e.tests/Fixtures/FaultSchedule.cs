using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.InteropServices;

namespace Squirix.E2ETests.Fixtures;

/// <summary>
/// A seeded pseudo-random sequence of faults over the nodes of a cluster: a node stops, shuts down abruptly, restarts or is isolated, and the
/// cut links heal. The schedule never touches the anchor node, and never keeps more than a minority of the replicas stopped or isolated at once,
/// so a majority stays alive and connected after every step.
/// </summary>
/// <remarks>The same seed yields the same sequence for the same inputs; the schedule keeps its own record of which nodes are down.</remarks>
internal sealed class FaultSchedule
{
    /// <summary>The name of the environment variable that overrides the seed of every schedule.</summary>
    internal const string SeedVariable = "SQUIRIX_FAULT_SEED";

    private readonly string _anchor;
    private readonly List<string> _isolated = [];
    private readonly int _limit;
    private readonly string[] _nodes;
    private readonly List<string> _stopped = [];
    private ulong _state;

    /// <summary>Initializes a new instance of the <see cref="FaultSchedule" /> class.</summary>
    /// <param name="nodes">The nodes of the cluster, in ring order.</param>
    /// <param name="anchor">The node that is never faulted, so clients can stay connected to it.</param>
    /// <param name="replicaCount">The replica factor, which sets the minority the schedule may take down at once.</param>
    /// <param name="seed">The seed of the sequence, which drives a SplitMix64 generator: unlike <see cref="Random" />, its sequence does not change between runtime versions.</param>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null" />.</exception>
    /// <exception cref="ArgumentException"><paramref name="anchor" /> is not one of <paramref name="nodes" />.</exception>
    internal FaultSchedule(string[] nodes, string anchor, int replicaCount, ulong seed)
    {
        ArgumentNullException.ThrowIfNull(nodes);
        ArgumentNullException.ThrowIfNull(anchor);
        if (Array.IndexOf(nodes, anchor) < 0)
            throw new ArgumentException($"The anchor {anchor} is not one of the nodes.", nameof(anchor));

        _nodes = nodes;
        _anchor = anchor;
        _limit = (replicaCount - 1) / 2;
        _state = seed;
    }

    /// <summary>Gets the seed to use for a schedule: the value of <see cref="SeedVariable" /> when set, otherwise a stable hash of the name.</summary>
    /// <param name="name">The name of the test case.</param>
    /// <returns>The seed.</returns>
    /// <exception cref="FormatException">The variable is set and is not an unsigned integer.</exception>
    internal static ulong SeedFor(string name)
    {
        var text = Environment.GetEnvironmentVariable(SeedVariable);
        return string.IsNullOrWhiteSpace(text) ? FailoverTiming.SeedOf(name) : ulong.Parse(text, NumberStyles.None, CultureInfo.InvariantCulture);
    }

    /// <summary>Gets the nodes that are running and connected.</summary>
    /// <returns>The nodes, in ring order.</returns>
    internal string[] Healthy() => Select(true);

    /// <summary>Gets the nodes that are running, isolated ones included.</summary>
    /// <returns>The nodes, in ring order.</returns>
    internal string[] Running() => Select(false);

    /// <summary>Hands out the steps that bring every stopped node back and restore every link, and clears the schedule.</summary>
    /// <returns>The steps, restarts first.</returns>
    internal List<FaultStep> Drain()
    {
        var steps = new List<FaultStep>(_stopped.Count + 1);
        foreach (var node in CollectionsMarshal.AsSpan(_stopped))
            steps.Add(new FaultStep(FaultKind.Restart, node));

        if (_isolated.Count > 0)
            steps.Add(new FaultStep(FaultKind.Heal, string.Empty));

        _stopped.Clear();
        _isolated.Clear();
        return steps;
    }

    /// <summary>Chooses the next step and records its effect.</summary>
    /// <param name="leader">The leader of the watched group, or empty when none is known; a fault hits it about half of the time when it may.</param>
    /// <returns>The step to apply.</returns>
    internal FaultStep Next(string leader)
    {
        var kinds = new List<FaultKind>(5);
        if (_stopped.Count + _isolated.Count < _limit)
        {
            kinds.Add(FaultKind.Stop);
            kinds.Add(FaultKind.AbruptStop);
            kinds.Add(FaultKind.Isolate);
        }

        if (_stopped.Count > 0)
            kinds.Add(FaultKind.Restart);

        if (_isolated.Count > 0)
            kinds.Add(FaultKind.Heal);

        return Take(kinds[NextBelow(kinds.Count)], leader);
    }

    /// <summary>Applies a kind of fault to a node the schedule chooses, and records its effect.</summary>
    /// <param name="kind">The kind of fault.</param>
    /// <param name="leader">The leader of the watched group, or empty when none is known.</param>
    /// <returns>The step.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="kind" /> is not a defined kind.</exception>
    private FaultStep Take(FaultKind kind, string leader)
    {
        switch (kind)
        {
            case FaultKind.Restart:
                var restarted = _stopped[NextBelow(_stopped.Count)];
                _ = _stopped.Remove(restarted);
                return new FaultStep(kind, restarted);
            case FaultKind.Heal:
                _isolated.Clear();
                return new FaultStep(kind, string.Empty);
            case FaultKind.Stop:
            case FaultKind.AbruptStop:
                var stopped = Victim(leader);
                _stopped.Add(stopped);
                return new FaultStep(kind, stopped);
            case FaultKind.Isolate:
                var cut = Victim(leader);
                _isolated.Add(cut);
                return new FaultStep(kind, cut);
            default:
                throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported enum value.");
        }
    }

    /// <summary>Draws the next value of the SplitMix64 generator.</summary>
    /// <param name="bound">The exclusive upper bound, above zero.</param>
    /// <returns>A value from zero up to <paramref name="bound" />.</returns>
    private int NextBelow(int bound)
    {
        _state = unchecked(_state + 0x9E3779B97F4A7C15UL);
        var z = _state;
        z = unchecked((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL);
        z = unchecked((z ^ (z >> 27)) * 0x94D049BB133111EBUL);
        z ^= z >> 31;
        return int.CreateChecked(z % ulong.CreateChecked(bound));
    }

    private string[] Select(bool connectedOnly)
    {
        var selected = new List<string>(_nodes.Length);
        foreach (var node in _nodes)
        {
            if (!_stopped.Contains(node) && !(connectedOnly && _isolated.Contains(node)))
                selected.Add(node);
        }

        return [.. selected];
    }

    private string Victim(string leader)
    {
        var candidates = new List<string>(_nodes.Length);
        foreach (var node in Healthy())
        {
            if (!string.Equals(node, _anchor, StringComparison.Ordinal))
                candidates.Add(node);
        }

        var leads = candidates.Contains(leader);
        var pick = NextBelow(candidates.Count + (leads ? candidates.Count : 0));
        return leads && pick >= candidates.Count ? leader : candidates[pick % candidates.Count];
    }
}
