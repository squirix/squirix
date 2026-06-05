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

        var keyCount = StressLoadProfiles.ScaleOperations(50);
        var keys = new string[keyCount];
        for (var k = 0; k < keyCount; k++)
            keys[k] = string.Concat("mixed:", k.ToString(CultureInfo.InvariantCulture));

        await using var cluster = await E2ECluster.StartSingleNodeAsync(nameof(ConcurrentMixedMutationsKeepClientVisibleInvariants), token);

        var clients = await ConnectClientsAsync(cluster, profile.Writers, "nodeA", token);
        var caches = new ICache<object?>[clients.Count];
        for (var i = 0; i < clients.Count; i++)
            caches[i] = await clients[i].GetCacheAsync<object?>("orders", token);

        var addSuccesses = new int[keyCount];

        await RunWritersAsync(
            profile.Writers,
            async w =>
            {
                var cache = caches[w];
                for (var k = 0; k < keyCount; k++)
                {
                    if (await cache.TryAddAsync(keys[k], string.Concat("w", w.ToString(CultureInfo.InvariantCulture)), cancellationToken: token))
                        _ = Interlocked.Increment(ref addSuccesses[k]);
                }
            },
            profile.Budget);

        for (var k = 0; k < keyCount; k++)
            Assert.Equal(1, addSuccesses[k]);

        var expectedValues = new HashSet<string>(StringComparer.Ordinal);
        for (var w = 0; w < profile.Writers; w++)
            _ = expectedValues.Add(string.Concat("w", w.ToString(CultureInfo.InvariantCulture), "-v2"));

        await RunWritersAsync(
            profile.Writers,
            async w =>
            {
                var cache = caches[w];
                var value = string.Concat("w", w.ToString(CultureInfo.InvariantCulture), "-v2");
                for (var k = 0; k < keyCount; k++)
                    await cache.SetAsync(keys[k], value, cancellationToken: token);
            },
            profile.Budget);

        for (var k = 0; k < keyCount; k++)
        {
            var entry = await caches[0].GetEntryAsync(keys[k], token);
            Assert.True(entry.Found);
            Assert.Contains((string)entry.Value!, expectedValues);

            var reread = await caches[0].GetEntryAsync(keys[k], token);
            Assert.True(reread.Found);
            Assert.Equal(entry.Value, reread.Value);
        }
    }
}
