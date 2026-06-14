using System;
using System.Linq;
using FsCheck.Fluent;
using Squirix.Server.Cluster;
using Xunit;

namespace Squirix.Server.PropertyTests;

/// <summary>
/// Property-based test suite for <see cref="ConsistentHashRing" />.
/// Validates determinism (order-invariance), approximate fairness,
/// and minimal movement on topology changes.
/// </summary>
public sealed class ConsistentHashRingPropertyTests
{
    /// <summary>
    /// Executes the determinism property via FsCheck under xUnit v3.
    /// </summary>
    [Fact]
    public void DeterminismSameConfigYieldsIdenticalOwnersProperty() => RunProperty(
        60,
        static (nodes, keySeed, orderSeed, vnodes) => CheckDeterminismSameConfigYieldsIdenticalOwners(nodes, keySeed, orderSeed, vnodes));

    /// <summary>
    /// Executes the fairness property via FsCheck under xUnit v3.
    /// </summary>
    [Fact]
    public void FairDistributionIsApproximatelyUniformProperty() => RunProperty(
        60,
        static (nodes, seed, vnodes) => CheckFairDistributionIsApproximatelyUniform(nodes, seed, vnodes));

    /// <summary>
    /// Executes the add-node movement property via FsCheck under xUnit v3.
    /// </summary>
    [Fact]
    public void MovementOnAddIsUpperBoundedProperty() => RunProperty(30, static (baseNodes, seed, vnodes) => CheckMovementOnAddIsUpperBounded(baseNodes, seed, vnodes));

    /// <summary>
    /// Executes the remove-node movement property via FsCheck under xUnit v3.
    /// </summary>
    [Fact]
    public void MovementOnRemoveAffectsOnlyKeysOfRemovedNodeProperty() => RunProperty(
        40,
        static (nodes, seed, vnodes) => CheckMovementOnRemoveAffectsOnlyKeysOfRemovedNode(nodes, seed, vnodes));

    /// <summary>
    /// Verifies determinism of owner selection for the same configuration.
    /// Specifically, for identical node sets (regardless of enumeration order)
    /// and the same <paramref name="vnodes" />, <see cref="ConsistentHashRing.GetOwner(string)" />
    /// must return identical owners for all sampled keys and for repeated calls.
    /// </summary>
    /// <param name="nodes">
    /// Distinct node identifiers participating in the ring. May be empty; in that case the property vacuously holds.
    /// </param>
    /// <param name="keySeed">Seed for deterministic key sampling.</param>
    /// <param name="orderSeed">Seed used to shuffle <paramref name="nodes" /> for order-invariance checks.</param>
    /// <param name="vnodes">Number of virtual nodes per physical node.</param>
    private static void CheckDeterminismSameConfigYieldsIdenticalOwners(string[] nodes, int keySeed, int orderSeed, int vnodes)
    {
        if (nodes.Length == 0)
            return;

        var ringA = new ConsistentHashRing(nodes, vnodes);
        var ringB = new ConsistentHashRing(RingHelpers.Shuffle(nodes, orderSeed), vnodes);

        const int sample = 6_000;
        foreach (var key in RingHelpers.MakeKeys(sample, keySeed))
        {
            var a = ringA.GetOwner(key);
            var b = ringB.GetOwner(key);

            Assert.Equal(a, b);
            Assert.Equal(a, ringA.GetOwner(key));
        }
    }

    /// <summary>
    /// Verifies approximate fairness of distribution: with many random keys,
    /// per-node hit counts should be close to uniform. The acceptance threshold
    /// combines sampling noise (binomial variance) and ring discreteness
    /// (finite vnodes per node).
    /// </summary>
    /// <param name="nodes">Distinct node identifiers; requires at least two nodes for a meaningful split.</param>
    /// <param name="seed">Seed for deterministic key sampling.</param>
    /// <param name="vnodes">Number of virtual nodes per physical node.</param>
    private static void CheckFairDistributionIsApproximatelyUniform(string[] nodes, int seed, int vnodes)
    {
        if (nodes.Length < 2)
            return;

        var ring = new ConsistentHashRing(nodes, vnodes);
        const int sample = 50_000;

        var counts = nodes.ToDictionary(static n => n, static _ => 0, StringComparer.OrdinalIgnoreCase);
        foreach (var key in RingHelpers.MakeKeys(sample, seed))
            counts[ring.GetOwner(key)]++;

        var n = nodes.Length;
        var expected = sample / (double)n;
        var p = 1.0 / n;
        var sigma = Math.Sqrt(sample * p * (1 - p));
        var relSigma = sigma / expected;
        var samplingTerm = (relSigma * 8) + 0.02;
        var k = Math.Max(1.0, vnodes / (double)n);
        var discretenessTerm = 1.8 / Math.Sqrt(k);
        var smallClusterSlack = n switch
        {
            2 => 0.06,
            3 => 0.03,
            _ => 0.0,
        };
        var threshold = Math.Min(0.50, Math.Max(0.15, Math.Max(samplingTerm, discretenessTerm) + smallClusterSlack));
        var maxDev = counts.Values.Max(c => Math.Abs(c - expected) / expected);
        var userMessage = $"n={n}, vnodes={vnodes}, perNode≈{k:F1}, sample={sample}, " + $"maxDev={maxDev:P2}, threshold={threshold:P2}, " +
                          $"sampling={samplingTerm:P2}, discrete={discretenessTerm:P2}, " + $"counts=[{string.Join(", ", counts.Values)}]";
        Assert.True(maxDev <= threshold, userMessage);
    }

    /// <summary>
    /// Verifies minimal movement when adding a node: approximately a fraction of <c>1/(N+1)</c>
    /// of keys should change owners, where <c>N</c> is the number of nodes before adding.
    /// The assertion uses an upper bound (expected + slack) to catch regressions while allowing
    /// random variance and discrete vnode placement.
    /// </summary>
    /// <param name="baseNodes">
    /// The set of nodes before adding a new one. Must contain at least two distinct node identifiers.
    /// </param>
    /// <param name="seed">
    /// Seed controlling the identity of the synthetic new node (for reproducibility) and key sampling.
    /// </param>
    /// <param name="vnodes">
    /// Number of virtual nodes per physical node used to build the ring.
    /// </param>
    private static void CheckMovementOnAddIsUpperBounded(string[] baseNodes, int seed, int vnodes)
    {
        if (baseNodes.Length < 2)
            return;

        var ring = new ConsistentHashRing(baseNodes, vnodes);
        var newNode = $"node-new-{Math.Abs(seed % 1_000_000)}";
        var withNew = baseNodes.Append(newNode).ToArray();
        var ring2 = new ConsistentHashRing(withNew, vnodes);

        const int sample = 20_000;
        var moved = RingHelpers.MakeKeys(sample, seed).Count(key => !string.Equals(ring.GetOwner(key), ring2.GetOwner(key), StringComparison.OrdinalIgnoreCase));
        var n = baseNodes.Length;
        var ratio = moved / (double)sample;
        var expected = 1.0 / (n + 1);
        var upper = expected + 0.12;

        Assert.True(ratio <= upper, $"n={n}, expected~={expected:P1}, moved={ratio:P2} (<= {upper:P2}), vnodes={vnodes}");
    }

    /// <summary>
    /// Verifies minimal movement when removing a node:
    /// only keys owned by the removed node are allowed to change owners;
    /// all other keys must keep their previous owners.
    /// </summary>
    /// <param name="nodes">Nodes before removal; requires at least two nodes.</param>
    /// <param name="seed">Seed selecting the victim index and sampling keys.</param>
    /// <param name="vnodes">Number of virtual nodes per physical node.</param>
    private static void CheckMovementOnRemoveAffectsOnlyKeysOfRemovedNode(string[] nodes, int seed, int vnodes)
    {
        if (nodes.Length < 2)
            return;

        var ringBefore = new ConsistentHashRing(nodes, vnodes);
        var victimIndex = Math.Abs(seed % nodes.Length);
        var victim = nodes[victimIndex];
        var remaining = nodes.Where(n => !string.Equals(n, victim, StringComparison.OrdinalIgnoreCase)).ToArray();
        var ringAfter = new ConsistentHashRing(remaining, vnodes);

        const int phi = unchecked((int)0x9E3779B9u);
        var altSeed = seed ^ phi;

        const int sample = 20_000;
        foreach (var key in RingHelpers.MakeKeys(sample, altSeed))
        {
            var before = ringBefore.GetOwner(key);
            var after = ringAfter.GetOwner(key);

            if (!string.Equals(before, after, StringComparison.Ordinal))
                Assert.Equal(victim, before);
        }
    }

    private static void RunProperty(int maxTest, Action<string[], int, int> property)
    {
        foreach (var (nodes, seed, vnodes) in SampleInputs3(maxTest))
            property(nodes, seed, vnodes);
    }

    private static void RunProperty(int maxTest, Action<string[], int, int, int> property)
    {
        foreach (var (nodes, seedA, seedB, vnodes) in SampleInputs4(maxTest))
            property(nodes, seedA, seedB, vnodes);
    }

    private static (string[] Nodes, int Seed, int Vnodes)[] SampleInputs3(int count)
    {
        var generator = RingArbitraries.Nodes().Generator.SelectMany(static _ => Gen.Choose(int.MinValue, int.MaxValue), static (nodes, seed) => new { nodes, seed }).SelectMany(
            static _ => RingArbitraries.VirtualNodes().Generator,
            static (t, vnodes) => (t.nodes, t.seed, vnodes));

        return [.. generator.Sample(count)];
    }

    private static (string[] Nodes, int SeedA, int SeedB, int Vnodes)[] SampleInputs4(int count)
    {
        var generator = RingArbitraries.Nodes().Generator.SelectMany(static _ => Gen.Choose(int.MinValue, int.MaxValue), static (nodes, seedA) => new { nodes, seedA })
                                       .SelectMany(static _ => Gen.Choose(int.MinValue, int.MaxValue), static (t, seedB) => new { t, seedB }).SelectMany(
                                            static _ => RingArbitraries.VirtualNodes().Generator,
                                            static (t, vnodes) => (t.t.nodes, t.t.seedA, t.seedB, vnodes));

        return [.. generator.Sample(count)];
    }
}
