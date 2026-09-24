using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Node.App;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>Allocation cost of conditional durable mutations through <see cref="JournalLoggingCacheDecorator{T}" /> with group commit off.</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class JournalLoggingCacheDecoratorBenchmarks
{
    private const string ExistingCache = "existing";
    private const string Self = "node-a";
    private const int OperationsPerInvoke = 50_000;
    private JournalLoggingCacheDecorator<string>? _decorator;
    private NodeCacheEntry<string>? _entry;
    private JournalBenchmarkHost? _host;

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

    /// <summary>Creates the journal coordinator, executor, and decorator with an in-memory fake cache.</summary>
    /// <returns>A task that completes when setup finishes.</returns>
    [GlobalSetup]
    public async Task SetupAsync()
    {
        var options = new PersistenceOptions
        {
            JournalPlatformBackend = JournalPlatformBackend.RandomAccess,
            JournalMaxSegmentMb = 64,
        };
        _host = await JournalBenchmarkHost.CreateAsync("journal-decorator-bench", options, CancellationToken.None).ConfigureAwait(false);
        _decorator = new JournalLoggingCacheDecorator<string>(Self, new SelfLocator(), new FakeCache(), _host.Coordinator, new DurableMutationExecutor(_host.Coordinator));
        _entry = new NodeCacheEntry<string> { Value = "v" };
    }

    /// <summary>Runs conditional add mutations, whose precondition reads the cache before the journal append.</summary>
    /// <returns>A task that completes when all operations finish.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task ConditionalAddAsync()
    {
        var decorator = ThrowHelper.Required(_decorator, "Benchmark decorator was not initialized.");
        var entry = ThrowHelper.Required(_entry, "Benchmark entry was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
            _ = await decorator.TryAddEntryAsync("op", "bench", "key", entry, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Runs conditional update mutations, whose precondition reads the cache before the journal append.</summary>
    /// <returns>A task that completes when all operations finish.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task ConditionalUpdateAsync()
    {
        var decorator = ThrowHelper.Required(_decorator, "Benchmark decorator was not initialized.");
        for (var i = 0; i < OperationsPerInvoke; i++)
            _ = await decorator.UpdateAsync("op", ExistingCache, "key", "w", CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Owner locator that assigns every key to the local node.</summary>
    private sealed class SelfLocator : INodeLocator
    {
        public string GetOwner(string cacheName, string key) => Self;
    }

    /// <summary>Cache stub: keys are absent except in the existing cache, which makes update preconditions pass.</summary>
    private sealed class FakeCache : ILogicalNamespacedCache<string>
    {
        private static readonly NodeCacheEntry<string> Existing = new() { Value = "v" };

        public ValueTask<NodeCacheEntry<string>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            new(Existing);

        public ValueTask<NodeCacheValueResult<string>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            new(new NodeCacheValueResult<string>(string.Equals(cacheName, ExistingCache, StringComparison.Ordinal), null));

        public ValueTask<CacheRemoveResult<string>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken) =>
            new(true);

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, string? value, CancellationToken cancellationToken) =>
            new(true);
    }
}
