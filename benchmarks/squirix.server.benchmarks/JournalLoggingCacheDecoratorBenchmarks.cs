using System;
using System.Collections.Frozen;
using System.Collections.Generic;
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
/// Allocation cost of conditional durable mutations, touches and expiration removals through <see cref="JournalLoggingCacheDecorator{T}" />
/// with group commit off.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class JournalLoggingCacheDecoratorBenchmarks
{
    private const string ExistingCache = "existing";
    private const int LargeOperationsPerInvoke = 200;
    private const string LargeValueCache = "value-1mib";
    private const int MediumOperationsPerInvoke = 10_000;
    private const string MediumValueCache = "value-16kib";
    private const int OperationsPerInvoke = 50_000;
    private const string SmallValueCache = "value-128b";
    private static readonly TimeSpan TouchTtl = TimeSpan.FromMinutes(10);
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
        _decorator = new JournalLoggingCacheDecorator<string>(new FakeCache(), _host.Coordinator, new DurableMutationExecutor(_host.Coordinator, NullLogger<DurableMutationExecutor>.Instance));
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

    /// <summary>Runs touches of a 128 B entry, each journaling a put of the whole touched entry.</summary>
    /// <returns>A task that completes when all operations finish.</returns>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public Task Touch128BAsync() => TouchAsync(SmallValueCache, OperationsPerInvoke);

    /// <summary>Runs touches of a 16 KiB entry, each journaling a put of the whole touched entry.</summary>
    /// <returns>A task that completes when all operations finish.</returns>
    [Benchmark(OperationsPerInvoke = MediumOperationsPerInvoke)]
    public Task Touch16KiBAsync() => TouchAsync(MediumValueCache, MediumOperationsPerInvoke);

    /// <summary>Runs touches of a 1 MiB entry, each journaling a put of the whole touched entry.</summary>
    /// <returns>A task that completes when all operations finish.</returns>
    [Benchmark(OperationsPerInvoke = LargeOperationsPerInvoke)]
    public Task Touch1MiBAsync() => TouchAsync(LargeValueCache, LargeOperationsPerInvoke);

    /// <summary>Runs expiration removals of a 128 B entry, each journaling a put of the whole persisted entry.</summary>
    /// <returns>A task that completes when all operations finish.</returns>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public Task RemoveExpiration128BAsync() => RemoveExpirationAsync(SmallValueCache, OperationsPerInvoke);

    /// <summary>Runs expiration removals of a 16 KiB entry, each journaling a put of the whole persisted entry.</summary>
    /// <returns>A task that completes when all operations finish.</returns>
    [Benchmark(OperationsPerInvoke = MediumOperationsPerInvoke)]
    public Task RemoveExpiration16KiBAsync() => RemoveExpirationAsync(MediumValueCache, MediumOperationsPerInvoke);

    /// <summary>Runs expiration removals of a 1 MiB entry, each journaling a put of the whole persisted entry.</summary>
    /// <returns>A task that completes when all operations finish.</returns>
    [Benchmark(OperationsPerInvoke = LargeOperationsPerInvoke)]
    public Task RemoveExpiration1MiBAsync() => RemoveExpirationAsync(LargeValueCache, LargeOperationsPerInvoke);

    private async Task RemoveExpirationAsync(string cacheName, int operations)
    {
        var decorator = ThrowHelper.Required(_decorator, "Benchmark decorator was not initialized.");
        for (var i = 0; i < operations; i++)
            _ = await decorator.RemoveExpirationAsync("op", cacheName, "key", CancellationToken.None).ConfigureAwait(false);
    }

    private async Task TouchAsync(string cacheName, int operations)
    {
        var decorator = ThrowHelper.Required(_decorator, "Benchmark decorator was not initialized.");
        for (var i = 0; i < operations; i++)
            _ = await decorator.TouchAsync("op", cacheName, "key", TouchTtl, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Cache stub: keys are absent except in the existing cache, which makes update preconditions pass; the sized caches hold entries with a deadline.</summary>
    private sealed class FakeCache : ILogicalNamespacedCache<string>
    {
        private static readonly NodeCacheEntry<string> Existing = new() { Value = "v" };

        private static readonly FrozenDictionary<string, NodeCacheEntry<string>> Sized = new Dictionary<string, NodeCacheEntry<string>>(StringComparer.Ordinal)
        {
            [SmallValueCache] = CreateSized(128),
            [MediumValueCache] = CreateSized(16 * 1024),
            [LargeValueCache] = CreateSized(1024 * 1024),
        }.ToFrozenDictionary(StringComparer.Ordinal);

        public ValueTask<NodeCacheEntry<string>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            new(Sized.TryGetValue(cacheName, out var sized) ? sized : Existing);

        public ValueTask<NodeCacheValueResult<string>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            new(new NodeCacheValueResult<string>(string.Equals(cacheName, ExistingCache, StringComparison.Ordinal), null));

        public ValueTask<CacheRemoveResult<string>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken) =>
            new(true);

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, string? value, CancellationToken cancellationToken) =>
            new(true);

        private static NodeCacheEntry<string> CreateSized(int valueBytes) => new() { Value = new string('x', valueBytes), ExpiresUtc = DateTime.UtcNow.AddDays(1) };
    }
}
