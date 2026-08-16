using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.E2EBenchmarks.Fixtures;
using Squirix.E2EBenchmarks.Scenarios;
using Squirix.E2EBenchmarks.Support.Cluster;

namespace Squirix.E2EBenchmarks.Support.Harness;

/// <summary>Factory for typed value adapters used by parameterized benchmarks.</summary>
internal static class E2EBenchmarkValueAdapter
{
    internal static async Task<IE2EBenchmarkValueAdapter> CreateAsync(E2EBenchmarkCluster cluster, BenchmarkValueShape shape, string cacheName, CancellationToken cancellationToken)
    {
        return shape switch
        {
            BenchmarkValueShape.PrimitiveLong => new Adapter<long>(
                await cluster.GetCacheAsync<long>(cacheName, cancellationToken).ConfigureAwait(false),
                E2EBenchmarkDataFactory.CreateLong),
            BenchmarkValueShape.SmallString => new Adapter<string>(
                await cluster.GetCacheAsync<string>(cacheName, cancellationToken).ConfigureAwait(false),
                E2EBenchmarkDataFactory.CreateSmallString),
            BenchmarkValueShape.SmallCustomRecord => new Adapter<BenchmarkUserProfile>(
                await cluster.GetCacheAsync<BenchmarkUserProfile>(cacheName, cancellationToken).ConfigureAwait(false),
                E2EBenchmarkDataFactory.CreateUserProfile),
            BenchmarkValueShape.NestedCustomClass => new Adapter<BenchmarkOrder>(
                await cluster.GetCacheAsync<BenchmarkOrder>(cacheName, cancellationToken).ConfigureAwait(false),
                E2EBenchmarkDataFactory.CreateOrder),
            _ => throw new ArgumentOutOfRangeException(nameof(shape), shape, null),
        };
    }

    [Immutable]
    private sealed class Adapter<T> : IE2EBenchmarkValueAdapter
    {
        private readonly ICache<T> _cache;
        private readonly Func<int, T> _factory;
        private readonly GetOrAddHitProbe _getOrAddHitProbe;
        private readonly GetOrAddMissFactory _getOrAddMissFactory;

        internal Adapter(ICache<T> cache, Func<int, T> factory)
        {
            _cache = cache;
            _factory = factory;
            _getOrAddHitProbe = new GetOrAddHitProbe(factory);
            _getOrAddMissFactory = new GetOrAddMissFactory(factory);
        }

        public Task AddAsync(string key, int valueIndex, CancellationToken cancellationToken) => _cache.AddAsync(key, _factory(valueIndex), cancellationToken: cancellationToken);

        public async Task<bool> AddConflictAsync(string key, int valueIndex, CancellationToken cancellationToken)
        {
            try
            {
                await _cache.AddAsync(key, _factory(valueIndex), cancellationToken: cancellationToken).ConfigureAwait(false);
                return false;
            }
            catch (CacheConflictException)
            {
                return true;
            }
        }

        public async Task<bool> GetEntryHitAsync(string key, CancellationToken cancellationToken)
        {
            var result = await _cache.GetEntryAsync(key, cancellationToken).ConfigureAwait(false);
            return result is { Found: true, Value: not null };
        }

        public async Task<bool> GetExpirationAsync(string key, CancellationToken cancellationToken)
        {
            var result = await _cache.GetExpirationAsync(key, cancellationToken).ConfigureAwait(false);
            return result.Found;
        }

        public async Task<bool> GetOrAddHitAsync(string key, CancellationToken cancellationToken)
        {
            _getOrAddHitProbe.Reset();
            var result = await _cache.GetOrAddAsync(key, _getOrAddHitProbe.ValueFactory, cancellationToken: cancellationToken).ConfigureAwait(false);
            return result.Found && !_getOrAddHitProbe.Called;
        }

        public async Task<bool> GetOrAddMissAsync(string key, int valueIndex, CancellationToken cancellationToken)
        {
            _getOrAddMissFactory.ValueIndex = valueIndex;
            var result = await _cache.GetOrAddAsync(key, _getOrAddMissFactory.ValueFactory, cancellationToken: cancellationToken).ConfigureAwait(false);
            return result.Found;
        }

        public async Task<bool> GetValueHitAsync(string key, CancellationToken cancellationToken)
        {
            var result = await _cache.GetValueAsync(key, cancellationToken).ConfigureAwait(false);
            return result.Found;
        }

        public async Task<bool> GetValueMissAsync(string key, CancellationToken cancellationToken)
        {
            var result = await _cache.GetValueAsync(key, cancellationToken).ConfigureAwait(false);
            return !result.Found;
        }

        public Task<bool> RemoveAsync(string key, CancellationToken cancellationToken) => _cache.RemoveAsync(key, cancellationToken);

        public Task<bool> RemoveExpirationAsync(string key, CancellationToken cancellationToken) => _cache.RemoveExpirationAsync(key, cancellationToken);

        public async Task SeedAsync(string[] keys, CancellationToken cancellationToken)
        {
            for (var i = 0; i < keys.Length; i++)
                await _cache.SetAsync(keys[i], _factory(i), cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        public async Task SeedExpiringAsync(string[] keys, TimeSpan expiration, CancellationToken cancellationToken)
        {
            for (var i = 0; i < keys.Length; i++)
                await SetExpiringAsync(keys[i], i, expiration, cancellationToken).ConfigureAwait(false);
        }

        public Task SetAsync(string key, int valueIndex, CancellationToken cancellationToken) => _cache.SetAsync(key, _factory(valueIndex), cancellationToken: cancellationToken);

        public Task SetExpiringAsync(string key, int valueIndex, TimeSpan expiration, CancellationToken cancellationToken) => _cache.SetAsync(
            key,
            _factory(valueIndex),
            new CacheEntryOptions { Expiration = expiration },
            cancellationToken);

        public Task<bool> TouchAbsoluteAsync(string key, DateTimeOffset expiresAt, CancellationToken cancellationToken) => _cache.TouchAsync(key, expiresAt, cancellationToken);

        public Task<bool> TouchRelativeAsync(string key, TimeSpan expiration, CancellationToken cancellationToken) => _cache.TouchAsync(key, expiration, cancellationToken);

        public Task<bool> TryAddAsync(string key, int valueIndex, CancellationToken cancellationToken) =>
            _cache.TryAddAsync(key, _factory(valueIndex), cancellationToken: cancellationToken);

        public Task<bool> UpdateAsync(string key, int valueIndex, CancellationToken cancellationToken) => _cache.UpdateAsync(key, _factory(valueIndex), cancellationToken);

        private sealed class GetOrAddHitProbe
        {
            private readonly Func<int, T> _valueFactory;

            internal GetOrAddHitProbe(Func<int, T> valueFactory)
            {
                _valueFactory = valueFactory;
                ValueFactory = CreateValueAsync;
            }

            internal bool Called { get; private set; }

            internal Func<string, CancellationToken, Task<T?>> ValueFactory { get; }

            internal void Reset() => Called = false;

            private Task<T?> CreateValueAsync(string key, CancellationToken cancellationToken)
            {
                _ = key;
                cancellationToken.ThrowIfCancellationRequested();
                Called = true;
                return Task.FromResult<T?>(_valueFactory(-1));
            }
        }

        private sealed class GetOrAddMissFactory
        {
            private readonly Func<int, T> _valueFactory;

            internal GetOrAddMissFactory(Func<int, T> valueFactory)
            {
                _valueFactory = valueFactory;
                ValueFactory = CreateValueAsync;
            }

            internal Func<string, CancellationToken, Task<T?>> ValueFactory { get; }

            internal int ValueIndex { private get; set; }

            private Task<T?> CreateValueAsync(string key, CancellationToken cancellationToken)
            {
                _ = key;
                cancellationToken.ThrowIfCancellationRequested();
                return Task.FromResult<T?>(_valueFactory(ValueIndex));
            }
        }
    }
}
