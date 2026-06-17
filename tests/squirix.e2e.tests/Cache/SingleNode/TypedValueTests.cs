using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Fixtures.TypedValues;
using Squirix.E2ETests.Support.Cluster.Fixtures;
using Xunit;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>Integration tests for single-node typed custom values through the public cache API.</summary>
public sealed class TypedValueTests(SingleNodeFixture fixture) : SingleNodeTestBase(fixture)
{
    /// <summary>Verifies AddShouldThrowForExistingCustomRecordOnSingleNode.</summary>
    [Fact]
    public async Task AddShouldThrowForExistingCustomRecordOnSingleNode()
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-add", DefaultCancellationToken);
        var original = TypedValueFactory.CreateProfile("add-conflict");

        await cache.AddAsync("k", original, cancellationToken: DefaultCancellationToken);

        _ = await Assert.ThrowsAsync<CacheConflictException>(async () => await cache.AddAsync(
            "k",
            TypedValueFactory.CreateUpdatedProfile("add-conflict"),
            cancellationToken: DefaultCancellationToken));

        var result = await cache.GetValueAsync("k", DefaultCancellationToken);
        Assert.True(result.Found);
        TypedValueAssertions.AssertProfileEquals(original, result.Value!);
    }

    /// <summary>Verifies CustomRecordShouldRoundTripWithEmptyCollections.</summary>
    [Fact]
    public async Task CustomRecordShouldRoundTripWithEmptyCollections()
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-empty", DefaultCancellationToken);
        var expected = TypedValueFactory.CreateProfileWithEmptyCollections("empty");

        await cache.SetAsync("k", expected, cancellationToken: DefaultCancellationToken);

        var result = await cache.GetValueAsync("k", DefaultCancellationToken);
        Assert.True(result.Found);
        TypedValueAssertions.AssertProfileEquals(expected, result.Value!);
    }

    /// <summary>Verifies CustomRecordShouldRoundTripWithNullValueProperty.</summary>
    [Fact]
    public async Task CustomRecordShouldRoundTripWithNullValueProperty()
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-null", DefaultCancellationToken);
        var expected = TypedValueFactory.CreateProfileWithNullEmail("null-email");

        await cache.SetAsync("k", expected, cancellationToken: DefaultCancellationToken);

        var result = await cache.GetValueAsync("k", DefaultCancellationToken);
        Assert.True(result.Found);
        TypedValueAssertions.AssertProfileEquals(expected, result.Value!);
    }

    /// <summary>Verifies CustomRecordShouldRoundTripWithUnicodeText.</summary>
    [Fact]
    public async Task CustomRecordShouldRoundTripWithUnicodeText()
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-unicode", DefaultCancellationToken);
        var expected = TypedValueFactory.CreateProfileWithUnicodeText("unicode");

        await cache.SetAsync("k", expected, cancellationToken: DefaultCancellationToken);

        var result = await cache.GetValueAsync("k", DefaultCancellationToken);
        Assert.True(result.Found);
        TypedValueAssertions.AssertProfileEquals(expected, result.Value!);
    }

    /// <summary>Verifies GetEntryShouldReturnTypedValueAndMetadataOnSingleNode.</summary>
    [Fact]
    public async Task GetEntryShouldReturnTypedValueAndMetadataOnSingleNode()
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

    /// <summary>Verifies GetOrAddShouldStoreFactoryProducedCustomRecordOnSingleNode.</summary>
    [Fact]
    public async Task GetOrAddShouldStoreFactoryProducedCustomRecordOnSingleNode()
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-get-or-add", DefaultCancellationToken);
        var expected = TypedValueFactory.CreateProfile("k");
        var factoryCalls = 0;

        var first = await cache.GetOrAddAsync(
            "k",
            static (key, _) => Task.FromResult<TypedCustomerProfile?>(TypedValueFactory.CreateProfile(key)),
            cancellationToken: DefaultCancellationToken);

        var second = await cache.GetOrAddAsync(
            "k",
            (_, _) =>
            {
                _ = Interlocked.Increment(ref factoryCalls);
                return Task.FromResult<TypedCustomerProfile?>(TypedValueFactory.CreateUpdatedProfile("get-or-add"));
            },
            cancellationToken: DefaultCancellationToken);

        Assert.True(first.Found);
        TypedValueAssertions.AssertProfileEquals(expected, first.Value!);
        Assert.True(second.Found);
        TypedValueAssertions.AssertProfileEquals(expected, second.Value!);
        Assert.Equal(0, factoryCalls);
    }

    /// <summary>Verifies RemoveExpirationShouldClearExpirationForCustomRecordOnSingleNode.</summary>
    [Fact]
    public async Task RemoveExpirationShouldClearExpirationForCustomRecordOnSingleNode()
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

    /// <summary>Verifies SetAndGetShouldRoundTripCustomRecordOnSingleNode.</summary>
    [Fact]
    public async Task SetAndGetShouldRoundTripCustomRecordOnSingleNode()
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-record", DefaultCancellationToken);
        var expected = TypedValueFactory.CreateProfile("record");

        await cache.SetAsync("k", expected, cancellationToken: DefaultCancellationToken);

        var result = await cache.GetValueAsync("k", DefaultCancellationToken);
        Assert.True(result.Found);
        TypedValueAssertions.AssertProfileEquals(expected, result.Value!);
    }

    /// <summary>Verifies SetAndGetShouldRoundTripMutableClassOnSingleNode.</summary>
    [Fact]
    public async Task SetAndGetShouldRoundTripMutableClassOnSingleNode()
    {
        var cache = await Client.GetCacheAsync<TypedMutableCart>("typed-single-cart", DefaultCancellationToken);
        var expected = TypedValueFactory.CreateCart("cart");

        await cache.SetAsync("k", expected, cancellationToken: DefaultCancellationToken);

        var result = await cache.GetValueAsync("k", DefaultCancellationToken);
        Assert.True(result.Found);
        TypedValueAssertions.AssertCartEquals(expected, result.Value!);
    }

    /// <summary>Verifies TouchShouldUpdateExpirationForCustomRecordOnSingleNode.</summary>
    [Fact]
    public async Task TouchShouldUpdateExpirationForCustomRecordOnSingleNode()
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

    /// <summary>Verifies TryAddShouldReturnFalseForExistingCustomRecordOnSingleNode.</summary>
    [Fact]
    public async Task TryAddShouldReturnFalseForExistingCustomRecordOnSingleNode()
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-try-add", DefaultCancellationToken);
        var original = TypedValueFactory.CreateProfile("try-add");

        Assert.True(await cache.TryAddAsync("k", original, cancellationToken: DefaultCancellationToken));
        Assert.False(await cache.TryAddAsync("k", TypedValueFactory.CreateUpdatedProfile("try-add"), cancellationToken: DefaultCancellationToken));

        var result = await cache.GetValueAsync("k", DefaultCancellationToken);
        Assert.True(result.Found);
        TypedValueAssertions.AssertProfileEquals(original, result.Value!);
    }

    /// <summary>Verifies UpdateShouldPreserveExpirationForCustomRecordOnSingleNode.</summary>
    [Fact]
    public async Task UpdateShouldPreserveExpirationForCustomRecordOnSingleNode()
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
}
