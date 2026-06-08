using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Infrastructure;
using Squirix.E2ETests.Infrastructure.Stress;
using Xunit;

namespace Squirix.E2ETests.Stress.SingleNode;

/// <summary>
/// Concurrent mixed-mutation contention over a fixed key set, asserting client-visible correctness invariants.
/// </summary>
[Trait(StressCategory.TraitName, StressCategory.TraitValue)]
public sealed class MixedMutationStressTests : StressE2ETestBase
{
    /// <summary>
    /// Races concurrent TryAdd then Insert over a shared key set and asserts a single add winner per key
    /// and a converged final value drawn from the writer set.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task ConcurrentMixedMutationsKeepClientVisibleInvariants()
    {
        var profile = StressLoadProfiles.MixedMutation;
        using var deadline = CreateDeadline(profile);
        var token = deadline.Token;

        var keys = CreateKeySet(StressLoadProfiles.ScaleOperations(50));
        await using var cluster = await E2ECluster.StartSingleNodeAsync(nameof(ConcurrentMixedMutationsKeepClientVisibleInvariants), token);

        var caches = await ConnectOrderCachesAsync(cluster, profile.Writers, token);
        var addSuccesses = await RunTryAddContentionAsync(caches, keys, profile, token);
        AssertSingleTryAddWinnerPerKey(keys, addSuccesses);

        var expectedValues = BuildWriterValues(profile.Writers, "-v2");
        await RunInsertContentionAsync(caches, keys, profile, token);
        await AssertConvergedValuesAsync(caches[0], keys, expectedValues, token);
    }

    private static string[] CreateKeySet(int keyCount)
    {
        var keys = new string[keyCount];
        for (var k = 0; k < keyCount; k++)
            keys[k] = string.Concat("mixed:", k.ToString(CultureInfo.InvariantCulture));

        return keys;
    }

    private static async Task<ICache<object?>[]> ConnectOrderCachesAsync(E2ECluster cluster, int writers, CancellationToken token)
    {
        var clients = await ConnectClientsAsync(cluster, writers, "nodeA", token);
        var caches = new ICache<object?>[clients.Count];
        for (var i = 0; i < clients.Count; i++)
            caches[i] = await clients[i].GetCacheAsync<object?>("orders", token);

        return caches;
    }

    private static async Task<int[]> RunTryAddContentionAsync(
        ICache<object?>[] caches,
        string[] keys,
        StressLoadProfile profile,
        CancellationToken token)
    {
        var addSuccesses = new int[keys.Length];
        await RunWritersAsync(
            profile.Writers,
            async w => await TryAddKeysFromWriterAsync(caches[w], keys, w, addSuccesses, token),
            profile.Budget);

        return addSuccesses;
    }

    private static async Task TryAddKeysFromWriterAsync(
        ICache<object?> cache,
        string[] keys,
        int writer,
        int[] addSuccesses,
        CancellationToken token)
    {
        var value = string.Concat("w", writer.ToString(CultureInfo.InvariantCulture));
        for (var k = 0; k < keys.Length; k++)
        {
            if (await cache.TryAddAsync(keys[k], value, cancellationToken: token))
                _ = Interlocked.Increment(ref addSuccesses[k]);
        }
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
            _ = expectedValues.Add(string.Concat("w", w.ToString(CultureInfo.InvariantCulture), suffix));

        return expectedValues;
    }

    private static Task RunInsertContentionAsync(
        ICache<object?>[] caches,
        string[] keys,
        StressLoadProfile profile,
        CancellationToken token) =>
        RunWritersAsync(
            profile.Writers,
            async w => await SetKeysFromWriterAsync(caches[w], keys, w, token),
            profile.Budget);

    private static async Task SetKeysFromWriterAsync(ICache<object?> cache, string[] keys, int writer, CancellationToken token)
    {
        var value = string.Concat("w", writer.ToString(CultureInfo.InvariantCulture), "-v2");
        for (var k = 0; k < keys.Length; k++)
            await cache.SetAsync(keys[k], value, cancellationToken: token);
    }

    private static async Task AssertConvergedValuesAsync(
        ICache<object?> cache,
        string[] keys,
        HashSet<string> expectedValues,
        CancellationToken token)
    {
        for (var k = 0; k < keys.Length; k++)
            await AssertKeyConvergedAsync(cache, keys[k], expectedValues, token);
    }

    private static async Task AssertKeyConvergedAsync(
        ICache<object?> cache,
        string key,
        HashSet<string> expectedValues,
        CancellationToken token)
    {
        var entry = await cache.GetEntryAsync(key, token);
        Assert.True(entry.Found);
        Assert.Contains((string)entry.Value!, expectedValues);

        var reread = await cache.GetEntryAsync(key, token);
        Assert.True(reread.Found);
        Assert.Equal(entry.Value, reread.Value);
    }
}
