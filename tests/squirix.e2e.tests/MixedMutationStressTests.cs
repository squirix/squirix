using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Cluster;
using Xunit;

namespace Squirix.E2ETests;

/// <summary>Concurrent mixed-mutation contention over a fixed key set, asserting client-visible correctness invariants.</summary>
[Trait(Category.TraitName, Category.TraitValue)]
public sealed class MixedMutationStressTests : LoadTestBase
{
    /// <summary>
    /// Races concurrent TryAdd then Insert over a shared key set and asserts a single add winner per key
    /// and a converged final value drawn from the writer set.
    /// </summary>
    [Fact]
    public async Task ConcurrentMixedMutationsClientVisibleInvariants()
    {
        var profile = LoadProfiles.MixedMutation;
        using var deadline = CreateDeadline(profile);
        var token = deadline.Token;

        var keys = CreateKeySet(LoadProfiles.ScaleOperations(50));
        await using var cluster = await HostedCluster.StartSingleNodeAsync(nameof(ConcurrentMixedMutationsClientVisibleInvariants), cancellationToken: token);

        var caches = await ConnectOrderCachesAsync(cluster, profile.Writers, token);
        var addSuccesses = await RunTryAddContentionAsync(caches, keys, profile, token);
        AssertSingleTryAddWinnerPerKey(keys, addSuccesses);

        var expectedValues = BuildWriterValues(profile.Writers, "-v2");
        await RunInsertContentionAsync(caches, keys, profile, token);
        await AssertConvergedValuesAsync(caches[0], keys, expectedValues, token);
    }

    private static async Task AssertConvergedValuesAsync(ICache<object?> cache, string[] keys, HashSet<string> expectedValues, CancellationToken token)
    {
        for (var k = 0; k < keys.Length; k++)
            await AssertKeyConvergedAsync(cache, keys[k], expectedValues, token);
    }

    private static async Task AssertKeyConvergedAsync(ICache<object?> cache, string key, HashSet<string> expectedValues, CancellationToken token)
    {
        var entry = await cache.GetEntryAsync(key, token);
        Assert.True(entry.Found);
        Assert.Contains(Assert.IsType<string>(entry.Value), expectedValues);

        var reread = await cache.GetEntryAsync(key, token);
        Assert.True(reread.Found);
        Assert.Equal(entry.Value, reread.Value);
    }

    private static void AssertSingleTryAddWinnerPerKey(string[] keys, int[] addSuccesses)
    {
        for (var k = 0; k < keys.Length; k++)
            Assert.Equal(1, addSuccesses[k]);
    }

    private static HashSet<string> BuildWriterValues(int writers, string suffix)
    {
        var expectedValues = new HashSet<string>(StringComparer.Ordinal);
        for (var w = 0; w < writers; w++)
            _ = expectedValues.Add($"w{w.ToString(CultureInfo.InvariantCulture)}{suffix}");

        return expectedValues;
    }

    private static async Task<ICache<object?>[]> ConnectOrderCachesAsync(HostedCluster cluster, int writers, CancellationToken token)
    {
        var clients = await ConnectClientsAsync(cluster, writers, "nodeA", token);
        var caches = new ICache<object?>[clients.Count];
        for (var i = 0; i < clients.Count; i++)
            caches[i] = await clients[i].GetCacheAsync<object?>("orders", token);

        return caches;
    }

    private static string[] CreateKeySet(int keyCount)
    {
        var keys = new string[keyCount];
        for (var k = 0; k < keyCount; k++)
            keys[k] = $"mixed:{k.ToString(CultureInfo.InvariantCulture)}";

        return keys;
    }

    private static Task RunInsertContentionAsync(ICache<object?>[] caches, string[] keys, LoadProfile profile, CancellationToken token) => RunWritersAsync(
        profile.Writers,
        async w => await SetKeysFromWriterAsync(caches[w], keys, w, token),
        profile.Budget);

    private static async Task<int[]> RunTryAddContentionAsync(ICache<object?>[] caches, string[] keys, LoadProfile profile, CancellationToken token)
    {
        var addSuccesses = new int[keys.Length];
        await RunWritersAsync(profile.Writers, async w => await TryAddKeysFromWriterAsync(caches[w], keys, w, addSuccesses, token), profile.Budget);

        return addSuccesses;
    }

    private static async Task SetKeysFromWriterAsync(ICache<object?> cache, string[] keys, int writer, CancellationToken token)
    {
        var value = $"w{writer.ToString(CultureInfo.InvariantCulture)}-v2";
        for (var k = 0; k < keys.Length; k++)
            await cache.SetAsync(keys[k], value, cancellationToken: token);
    }

    private static async Task TryAddKeysFromWriterAsync(ICache<object?> cache, string[] keys, int writer, int[] addSuccesses, CancellationToken token)
    {
        var value = $"w{writer.ToString(CultureInfo.InvariantCulture)}";
        for (var k = 0; k < keys.Length; k++)
        {
            if (await cache.TryAddAsync(keys[k], value, cancellationToken: token))
                _ = Interlocked.Increment(ref addSuccesses[k]);
        }
    }

    /// <summary>
    /// Named stress workloads. Operation counts scale with <c>SQUIRIX_STRESS_SCALE</c> so the repeat runner can dial
    /// intensity without recompiling; DEBUG builds default to a low scale to keep local runs fast.
    /// </summary>
    private static class LoadProfiles
    {
        private const string ScaleVariable = "SQUIRIX_STRESS_SCALE";

        /// <summary>Gets the mixed-mutation contention workload over a fixed key set.</summary>
        internal static LoadProfile MixedMutation { get; } = new(6, TimeSpan.FromSeconds(120));

        /// <summary>Gets the effective operation-count multiplier.</summary>
        private static double Scale { get; } = ResolveScale();

        /// <summary>
        /// Scales a base operation count by <see cref="Scale" />, never returning less than one.
        /// </summary>
        /// <param name="baseOperations">The unscaled operation count.</param>
        /// <returns>The scaled operation count.</returns>
        internal static int ScaleOperations(int baseOperations)
        {
            var scaled = Convert.ToInt32(Math.Round(baseOperations * Scale, MidpointRounding.AwayFromZero));
            return Math.Max(1, scaled);
        }

        private static double ResolveScale()
        {
            var raw = Environment.GetEnvironmentVariable(ScaleVariable);
            if (!string.IsNullOrWhiteSpace(raw) && double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed) && parsed > 0d)
                return parsed;

#if DEBUG
            return 0.1d;
#else
            return 1d;
#endif
        }
    }
}
