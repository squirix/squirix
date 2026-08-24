using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.E2ETests.Fixtures.TypedValues;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>Integration tests for single-node typed custom values through the public cache API.</summary>
/// <param name="fixture">Shared single-node cluster fixture.</param>
[Immutable]
public sealed class TypedValueTests(SingleNodeFixture fixture) : TestBase(fixture)
{
    /// <summary>Verifies AddThrowsForExistingCustomRecord.</summary>
    [Fact]
    public async Task AddThrowsForExistingCustomRecord()
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-add", DefaultCancellationToken);
        var original = TypedValueFactory.CreateProfile("add-conflict");

        await cache.AddAsync("k", original, cancellationToken: DefaultCancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<CacheConflictException>(
            cache.AddAsync("k", TypedValueFactory.CreateUpdatedProfile("add-conflict"), cancellationToken: DefaultCancellationToken));

        var result = await cache.GetValueAsync("k", DefaultCancellationToken);
        Assert.True(result.Found);
        TypedValueAssertions.AssertProfileEquals(original, result.Value!);
    }

    /// <summary>Verifies RecordRoundTripsEmptyCollections.</summary>
    [Fact]
    public async Task RecordRoundTripsEmptyCollections()
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-empty", DefaultCancellationToken);
        var expected = TypedValueFactory.CreateProfileWithEmptyCollections("empty");

        await cache.SetAsync("k", expected, cancellationToken: DefaultCancellationToken);

        var result = await cache.GetValueAsync("k", DefaultCancellationToken);
        Assert.True(result.Found);
        TypedValueAssertions.AssertProfileEquals(expected, result.Value!);
    }

    /// <summary>Verifies RecordRoundTripsNullValueProperty.</summary>
    [Fact]
    public async Task RecordRoundTripsNullValueProperty()
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-null", DefaultCancellationToken);
        var expected = TypedValueFactory.CreateProfileWithNullEmail("null-email");

        await cache.SetAsync("k", expected, cancellationToken: DefaultCancellationToken);

        var result = await cache.GetValueAsync("k", DefaultCancellationToken);
        Assert.True(result.Found);
        TypedValueAssertions.AssertProfileEquals(expected, result.Value!);
    }

    /// <summary>Verifies RecordRoundTripsUnicodeText.</summary>
    [Fact]
    public async Task RecordRoundTripsUnicodeText()
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-unicode", DefaultCancellationToken);
        var expected = TypedValueFactory.CreateProfileWithUnicodeText("unicode");

        await cache.SetAsync("k", expected, cancellationToken: DefaultCancellationToken);

        var result = await cache.GetValueAsync("k", DefaultCancellationToken);
        Assert.True(result.Found);
        TypedValueAssertions.AssertProfileEquals(expected, result.Value!);
    }

    /// <summary>Verifies GetOrAddUsesFactoryProducedRecord.</summary>
    [Fact]
    public async Task GetOrAddUsesFactoryProducedRecord()
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-get-or-add", DefaultCancellationToken);
        var expected = TypedValueFactory.CreateProfile("k");
        var counter = new CallCounter();

        var first = await cache.GetOrAddAsync(
            "k",
            static (key, _) => Task.FromResult<TypedCustomerProfile?>(TypedValueFactory.CreateProfile(key)),
            cancellationToken: DefaultCancellationToken);

        var second = await cache.GetOrAddAsync("k", counter.CreateUpdatedProfileAsync, cancellationToken: DefaultCancellationToken);

        Assert.True(first.Found);
        TypedValueAssertions.AssertProfileEquals(expected, first.Value!);
        Assert.True(second.Found);
        TypedValueAssertions.AssertProfileEquals(expected, second.Value!);
        Assert.Equal(1, counter.Count);
    }

    /// <summary>Verifies GetEntryReturnsTypedValueAndMetadata.</summary>
    [Fact]
    public async Task GetEntryReturnsTypedValueAndMetadata()
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-entry", DefaultCancellationToken);
        var expected = TypedValueFactory.CreateProfile("entry");

        await cache.SetAsync("k", expected, new CacheEntryOptions { Expiration = TimeSpan.FromMinutes(5) }, DefaultCancellationToken);

        var entry = await cache.GetEntryAsync("k", DefaultCancellationToken);
        Assert.True(entry.Found);
        TypedValueAssertions.AssertProfileEquals(expected, entry.Value!);
        _ = Assert.NotNull(entry.ExpiresUtc);
        Assert.True(entry.ExpiresUtc > DateTime.UtcNow);
    }

    /// <summary>Verifies RemoveExpiryClearsRecordOnSingleNode.</summary>
    [Fact]
    public async Task RemoveExpiryClearsRecordOnSingleNode()
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-remove-expiration", DefaultCancellationToken);
        var expected = TypedValueFactory.CreateProfile("remove-expiration");

        await cache.SetAsync("k", expected, new CacheEntryOptions { Expiration = TimeSpan.FromMinutes(5) }, DefaultCancellationToken);

        Assert.True(await cache.RemoveExpirationAsync("k", DefaultCancellationToken));
        var expiration = await cache.GetExpirationAsync("k", DefaultCancellationToken);
        var result = await cache.GetValueAsync("k", DefaultCancellationToken);

        Assert.True(result.Found);
        Assert.False(expiration.HasExpiration);
        TypedValueAssertions.AssertProfileEquals(expected, result.Value!);
    }

    /// <summary>Verifies CustomRecordRoundTripsOnSingleNode.</summary>
    [Fact]
    public async Task CustomRecordRoundTripsOnSingleNode()
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-record", DefaultCancellationToken);
        var expected = TypedValueFactory.CreateProfile("record");

        await cache.SetAsync("k", expected, cancellationToken: DefaultCancellationToken);

        var result = await cache.GetValueAsync("k", DefaultCancellationToken);
        Assert.True(result.Found);
        TypedValueAssertions.AssertProfileEquals(expected, result.Value!);
    }

    /// <summary>Verifies MutableClassRoundTripsOnSingleNode.</summary>
    [Fact]
    public async Task MutableClassRoundTripsOnSingleNode()
    {
        var cache = await Client.GetCacheAsync<TypedMutableCart>("typed-single-cart", DefaultCancellationToken);
        var expected = TypedValueFactory.CreateCart("cart");

        await cache.SetAsync("k", expected, cancellationToken: DefaultCancellationToken);

        var result = await cache.GetValueAsync("k", DefaultCancellationToken);
        Assert.True(result.Found);
        TypedValueAssertions.AssertCartEquals(expected, result.Value!);
    }

    /// <summary>Verifies TouchUpdatesCustomRecordExpirySingleNode.</summary>
    [Fact]
    public async Task TouchUpdatesCustomRecordExpirySingleNode()
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-touch", DefaultCancellationToken);
        var expected = TypedValueFactory.CreateProfile("touch");

        await cache.SetAsync("k", expected, cancellationToken: DefaultCancellationToken);

        Assert.True(await cache.TouchAsync("k", TimeSpan.FromMinutes(2), DefaultCancellationToken));
        var expiration = await cache.GetExpirationAsync("k", DefaultCancellationToken);
        var result = await cache.GetValueAsync("k", DefaultCancellationToken);

        Assert.True(expiration.Found);
        Assert.True(expiration.HasExpiration);
        Assert.True(expiration.Expiration > TimeSpan.Zero);
        Assert.True(result.Found);
        TypedValueAssertions.AssertProfileEquals(expected, result.Value!);
    }

    /// <summary>Verifies TryAddReturnsFalseForExistingRecord.</summary>
    [Fact]
    public async Task TryAddReturnsFalseForExistingRecord()
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-try-add", DefaultCancellationToken);
        var original = TypedValueFactory.CreateProfile("try-add");

        Assert.True(await cache.TryAddAsync("k", original, cancellationToken: DefaultCancellationToken));
        Assert.False(await cache.TryAddAsync("k", TypedValueFactory.CreateUpdatedProfile("try-add"), cancellationToken: DefaultCancellationToken));

        var result = await cache.GetValueAsync("k", DefaultCancellationToken);
        Assert.True(result.Found);
        TypedValueAssertions.AssertProfileEquals(original, result.Value!);
    }

    /// <summary>Verifies UpdatePreservesCustomRecordExpiry.</summary>
    [Fact]
    public async Task UpdatePreservesCustomRecordExpiry()
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-update", DefaultCancellationToken);
        var updated = TypedValueFactory.CreateUpdatedProfile("update");

        await cache.SetAsync("k", TypedValueFactory.CreateProfile("update"), new CacheEntryOptions { Expiration = TimeSpan.FromMinutes(5) }, DefaultCancellationToken);

        Assert.True(await cache.UpdateAsync("k", updated, DefaultCancellationToken));
        var result = await cache.GetValueAsync("k", DefaultCancellationToken);
        var expiration = await cache.GetExpirationAsync("k", DefaultCancellationToken);

        Assert.True(result.Found);
        Assert.True(expiration.HasExpiration);
        TypedValueAssertions.AssertProfileEquals(updated, result.Value!);
    }

    private sealed class CallCounter
    {
        private int _count;

        internal int Count => _count;

        internal Task<TypedCustomerProfile?> CreateUpdatedProfileAsync(string key, CancellationToken cancellationToken)
        {
            _ = key;
            _ = cancellationToken;
            _ = Interlocked.Increment(ref _count);
            return Task.FromResult<TypedCustomerProfile?>(TypedValueFactory.CreateUpdatedProfile("get-or-add"));
        }
    }
}
