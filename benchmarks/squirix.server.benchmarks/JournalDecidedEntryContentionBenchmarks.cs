using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Core;
using Squirix.Server.Node.App;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>
/// Latency of a small set on one key while another key is touched or updated with a 1 MiB value in a loop, with group commit on. The decided
/// entry of a touch is encoded under the mutation gate, so the gap to the run without the concurrent mutation is the gate hold it adds.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 5)]
public class JournalDecidedEntryContentionBenchmarks
{
    private const int LargeValueBytes = 1024 * 1024;
    private const string LargeCache = "large";
    private const int OperationsPerInvoke = 500;
    private const string SmallCache = "small";
    private static readonly TimeSpan ContenderPause = TimeSpan.FromMilliseconds(10);
    private JournalLoggingCacheDecorator<string>? _decorator;
    private JournalBenchmarkHost? _host;
    private string _largeValue = string.Empty;
    private NodeCacheEntry<string>? _smallEntry;

    /// <summary>Disposes the journal coordinator created during setup.</summary>
    /// <returns>A task that completes when cleanup finishes.</returns>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        if (_host != null)
            await _host.DisposeAsync().ConfigureAwait(false);
        _host = null;
        _decorator = null;
    }

    /// <summary>Creates the group-commit journal, the executor and the decorator over a stub cache.</summary>
    /// <returns>A task that completes when setup finishes.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        var options = new PersistenceOptions
        {
            JournalGroupCommitMaxWait = TimeSpan.FromMilliseconds(1),
            JournalGroupCommitMaxBatch = 32,
            JournalMaxSegmentMb = 64,
            JournalMaxSegmentCount = 512,
            JournalMaxTotalBytesMb = 16 * 1024,
        };
        _host = await JournalBenchmarkHost.CreateAsync("journal-decided-contention-bench", options, CancellationToken.None).ConfigureAwait(false);
        _largeValue = new string('x', LargeValueBytes);
        _decorator = new JournalLoggingCacheDecorator<string>(
            new StubCache(_largeValue),
            _host.Coordinator,
            new DurableMutationExecutor(_host.Coordinator, NullLogger<DurableMutationExecutor>.Instance));
        _smallEntry = new NodeCacheEntry<string> { Value = "v" };
    }

    /// <summary>Runs small sets with nothing else contending for the mutation gate.</summary>
    /// <returns>A task that completes when all sets finish.</returns>
    [Benchmark(Baseline = true, OperationsPerInvoke = OperationsPerInvoke)]
    public Task SetAloneAsync() => SetWhileAsync(null);

    /// <summary>Runs small sets while a 1 MiB entry is touched in a loop on another key.</summary>
    /// <returns>A task that completes when all sets finish.</returns>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public Task SetWithTouch1MiBAsync() => SetWhileAsync(static (decorator, ct) => decorator.TouchAsync("op", LargeCache, "key", TimeSpan.FromMinutes(10), ct));

    /// <summary>Runs small sets while a 1 MiB entry is replaced by an update in a loop on another key.</summary>
    /// <returns>A task that completes when all sets finish.</returns>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public Task SetWithUpdate1MiBAsync()
    {
        var value = _largeValue;
        return SetWhileAsync((decorator, ct) => decorator.UpdateAsync("op", LargeCache, "key", value, ct));
    }

    private static async Task ContendAsync(
        JournalLoggingCacheDecorator<string> decorator,
        Func<JournalLoggingCacheDecorator<string>, CancellationToken, ValueTask<bool>> contender,
        CancellationToken stopToken)
    {
        await Task.Yield();
        while (!stopToken.IsCancellationRequested)
        {
            _ = await contender(decorator, CancellationToken.None).ConfigureAwait(false);
            await Task.Delay(ContenderPause, TimeProvider.System, CancellationToken.None).ConfigureAwait(false);
        }
    }

    private async Task SetWhileAsync(Func<JournalLoggingCacheDecorator<string>, CancellationToken, ValueTask<bool>>? contender)
    {
        var decorator = ThrowHelper.Required(_decorator, "Benchmark decorator was not initialized.");
        var entry = ThrowHelper.Required(_smallEntry, "Benchmark entry was not initialized.");
        using var stop = new CancellationTokenSource();
        var stopToken = stop.Token;
        var contention = contender == null ? Task.CompletedTask : ContendAsync(decorator, contender, stopToken);
        try
        {
            for (var i = 0; i < OperationsPerInvoke; i++)
                await decorator.SetEntryAsync("op", SmallCache, "key", entry, CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            await stop.CancelAsync().ConfigureAwait(false);
            await contention.ConfigureAwait(false);
        }
    }

    /// <summary>Cache stub that holds one 1 MiB entry with a deadline in the large cache and accepts every write.</summary>
    private sealed class StubCache : ILogicalNamespacedCache<string>
    {
        private readonly NodeCacheEntry<string> _large;

        internal StubCache(string largeValue)
        {
            _large = new NodeCacheEntry<string> { Value = largeValue, ExpiresUtc = DateTime.UtcNow.AddDays(1) };
        }

        public ValueTask<NodeCacheEntry<string>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => new(_large);

        public ValueTask<NodeCacheValueResult<string>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            new(new NodeCacheValueResult<string>(true, null));

        public ValueTask<CacheRemoveResult<string>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, string? value, CancellationToken cancellationToken) =>
            throw new NotSupportedException();
    }
}
