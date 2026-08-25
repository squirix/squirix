using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.E2ETests.Cluster;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.E2ETests;

/// <summary>Concurrent mixed-mutation contention over a fixed key set, asserting client-visible correctness invariants.</summary>
[Trait(Category.TraitName, Category.TraitValue)]
[Immutable]
public sealed class MixedMutationStressTests : LoadTestBase
{
    private static readonly string[] WriterValues = CreateWriterValues(string.Empty);

    private static readonly string[] WriterValuesV2 = CreateWriterValues("-v2");

    /// <summary>
    /// Races concurrent TryAdd then Insert over a shared key set and asserts a single add winner per key
    /// and a converged final value drawn from the writer set.
    /// </summary>
    [Fact]
    public async Task ConcurrentMixedMutationsStayConsistent()
    {
        var profile = LoadProfiles.MixedMutation;
        using var deadline = CreateDeadline(profile);
        var token = deadline.Token;

        var keys = CreateKeySet(LoadProfiles.ScaleOperations(50));
        const string name = nameof(ConcurrentMixedMutationsStayConsistent);
        await using var cluster = await HostedCluster.StartSingleNodeAsync(name, timeProvider: TimeProvider.System, cancellationToken: token);

        var caches = await ConnectOrderCachesAsync(cluster, profile.Writers, token);
        var addSuccesses = await RunTryAddContentionAsync(caches, keys, profile, token);
        AssertSingleTryAddWinnerPerKey(keys, addSuccesses);

        var expectedValues = BuildWriterValues(profile.Writers, WriterValuesV2);
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

    private static HashSet<string> BuildWriterValues(int writers, string[] writerValues)
    {
        var expectedValues = new HashSet<string>(StringComparer.Ordinal);
        for (var w = 0; w < writers; w++)
            _ = expectedValues.Add(writerValues[w]);

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
            keys[k] = NodeInvariantIndexStrings.FormatPrefixed("mixed", k);

        return keys;
    }

    private static string[] CreateWriterValues(string suffix)
    {
        var values = new string[32];
        for (var w = 0; w < values.Length; w++)
            values[w] = $"w{NodeInvariantIndexStrings.Format(w)}{suffix}";

        return values;
    }

    private static Task RunInsertContentionAsync(ICache<object?>[] caches, string[] keys, LoadProfile profile, CancellationToken token)
    {
        var runner = new InsertContentionRunner(caches, keys, WriterValuesV2, token);
        return RunWritersAsync(profile.Writers, runner.RunAsync, profile.Budget);
    }

    private static async Task<int[]> RunTryAddContentionAsync(ICache<object?>[] caches, string[] keys, LoadProfile profile, CancellationToken token)
    {
        var addSuccesses = new int[keys.Length];
        var runner = new TryAddContentionRunner(caches, keys, WriterValues, addSuccesses, token);
        await RunWritersAsync(profile.Writers, runner.RunAsync, profile.Budget);

        return addSuccesses;
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

    [Immutable]
    private sealed class InsertContentionRunner
    {
        private readonly ICache<object?>[] _caches;
        private readonly string[] _keys;
        private readonly CancellationToken _token;
        private readonly string[] _writerValues;

        internal InsertContentionRunner(ICache<object?>[] caches, string[] keys, string[] writerValues, CancellationToken token)
        {
            _caches = caches;
            _keys = keys;
            _writerValues = writerValues;
            _token = token;
            RunAsync = RunCoreAsync;
        }

        internal Func<int, Task> RunAsync { get; }

        private async Task RunCoreAsync(int writer)
        {
            var cache = _caches[writer];
            var value = _writerValues[writer];
            for (var k = 0; k < _keys.Length; k++)
                await cache.SetAsync(_keys[k], value, cancellationToken: _token);
        }
    }

    [Immutable]
    private sealed class TryAddContentionRunner
    {
        private readonly int[] _addSuccesses;
        private readonly ICache<object?>[] _caches;
        private readonly string[] _keys;
        private readonly CancellationToken _token;
        private readonly string[] _writerValues;

        internal TryAddContentionRunner(ICache<object?>[] caches, string[] keys, string[] writerValues, int[] addSuccesses, CancellationToken token)
        {
            _caches = caches;
            _keys = keys;
            _writerValues = writerValues;
            _addSuccesses = addSuccesses;
            _token = token;
            RunAsync = RunCoreAsync;
        }

        internal Func<int, Task> RunAsync { get; }

        private async Task RunCoreAsync(int writer)
        {
            var cache = _caches[writer];
            var value = _writerValues[writer];
            for (var k = 0; k < _keys.Length; k++)
            {
                if (await cache.TryAddAsync(_keys[k], value, cancellationToken: _token))
                    _ = Interlocked.Increment(ref _addSuccesses[k]);
            }
        }
    }
}
