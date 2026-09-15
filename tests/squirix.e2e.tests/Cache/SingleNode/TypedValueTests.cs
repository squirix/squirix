using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.E2ETests.Fixtures.TypedValues;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>Integration tests for single-node typed custom values through the public cache API.</summary>
[Immutable]
public sealed class TypedValueTests : TestBase
{
    /// <summary>Verifies TryAddReturnsFalseForExistingRecord.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddExistingKeepsOriginal(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-try-add", cancellationToken);
        var original = TypedValueFactory.CreateProfile("try-add");
        _ = await Assert.That(await cache.TryAddAsync("k", original, cancellationToken: cancellationToken)).IsTrue();
        _ = await Assert.That(await cache.TryAddAsync("k", TypedValueFactory.CreateUpdatedProfile("try-add"), cancellationToken: cancellationToken)).IsFalse();
        var result = await cache.GetValueAsync("k", cancellationToken);
        _ = await Assert.That(result.Found).IsTrue();
        await TypedValueAssertions.AssertProfileEquals(original, result.Value!);
    }

    /// <summary>Verifies AddThrowsForExistingCustomRecord.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddThrowsForExistingCustomRecord(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-add", cancellationToken);
        var original = TypedValueFactory.CreateProfile("add-conflict");
        await cache.AddAsync("k", original, cancellationToken: cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAsync<CacheConflictException>(
            cache.AddAsync("k", TypedValueFactory.CreateUpdatedProfile("add-conflict"), cancellationToken: cancellationToken));
        var result = await cache.GetValueAsync("k", cancellationToken);
        _ = await Assert.That(result.Found).IsTrue();
        await TypedValueAssertions.AssertProfileEquals(original, result.Value!);
    }

    /// <summary>Verifies CustomRecordRoundTripsOnSingleNode.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CustomRecordRoundTripsOnSingleNode(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-record", cancellationToken);
        var expected = TypedValueFactory.CreateProfile("record");
        await cache.SetAsync("k", expected, cancellationToken: cancellationToken);
        var result = await cache.GetValueAsync("k", cancellationToken);
        _ = await Assert.That(result.Found).IsTrue();
        await TypedValueAssertions.AssertProfileEquals(expected, result.Value!);
    }

    /// <summary>Verifies GetEntryReturnsTypedValueAndMetadata.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GetEntryReturnsTypedValueAndMetadata(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-entry", cancellationToken);
        var expected = TypedValueFactory.CreateProfile("entry");
        await cache.SetAsync("k", expected, Expiry.In(TimeSpan.FromMinutes(5)), cancellationToken);
        var entry = await cache.GetEntryAsync("k", cancellationToken);
        _ = await Assert.That(entry.Found).IsTrue();
        await TypedValueAssertions.AssertProfileEquals(expected, entry.Value!);
        _ = await Assert.That(entry.ExpiresUtc).IsNotNull();
        _ = await Assert.That(entry.ExpiresUtc > DateTime.UtcNow).IsTrue();
    }

    /// <summary>Verifies GetOrAddUsesFactoryProducedRecord.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GetOrAddUsesFactoryProducedRecord(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-get-or-add", cancellationToken);
        var expected = TypedValueFactory.CreateProfile("k");
        var counter = new CallCounter();
        var first = await cache.GetOrAddAsync(
            "k",
            static (key, _) => Task.FromResult<TypedCustomerProfile?>(TypedValueFactory.CreateProfile(key)),
            cancellationToken: cancellationToken);
        var second = await cache.GetOrAddAsync("k", counter.CreateUpdatedProfileAsync, cancellationToken: cancellationToken);
        _ = await Assert.That(first.Found).IsTrue();
        await TypedValueAssertions.AssertProfileEquals(expected, first.Value!);
        _ = await Assert.That(second.Found).IsTrue();
        await TypedValueAssertions.AssertProfileEquals(expected, second.Value!);
        _ = await Assert.That(counter.Count).IsEqualTo(1);
    }

    /// <summary>Verifies MutableClassRoundTripsOnSingleNode.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MutableClassRoundTripsOnSingleNode(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<TypedMutableCart>("typed-single-cart", cancellationToken);
        var expected = TypedValueFactory.CreateCart("cart");
        await cache.SetAsync("k", expected, cancellationToken: cancellationToken);
        var result = await cache.GetValueAsync("k", cancellationToken);
        _ = await Assert.That(result.Found).IsTrue();
        await TypedValueAssertions.AssertCartEquals(expected, result.Value!);
    }

    /// <summary>Verifies RecordRoundTripsEmptyCollections.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecordRoundTripsEmptyCollections(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-empty", cancellationToken);
        var expected = TypedValueFactory.CreateProfileWithEmptyCollections("empty");
        await cache.SetAsync("k", expected, cancellationToken: cancellationToken);
        var result = await cache.GetValueAsync("k", cancellationToken);
        _ = await Assert.That(result.Found).IsTrue();
        await TypedValueAssertions.AssertProfileEquals(expected, result.Value!);
    }

    /// <summary>Verifies RecordRoundTripsNullValueProperty.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecordRoundTripsNullValueProperty(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-null", cancellationToken);
        var expected = TypedValueFactory.CreateProfileWithNullEmail("null-email");
        await cache.SetAsync("k", expected, cancellationToken: cancellationToken);
        var result = await cache.GetValueAsync("k", cancellationToken);
        _ = await Assert.That(result.Found).IsTrue();
        await TypedValueAssertions.AssertProfileEquals(expected, result.Value!);
    }

    /// <summary>Verifies RecordRoundTripsUnicodeText.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecordRoundTripsUnicodeText(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-unicode", cancellationToken);
        var expected = TypedValueFactory.CreateProfileWithUnicodeText("unicode");
        await cache.SetAsync("k", expected, cancellationToken: cancellationToken);
        var result = await cache.GetValueAsync("k", cancellationToken);
        _ = await Assert.That(result.Found).IsTrue();
        await TypedValueAssertions.AssertProfileEquals(expected, result.Value!);
    }

    /// <summary>Verifies RemoveExpiryClearsRecordOnSingleNode.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpiryClearsRecordOnSingleNode(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-remove-expiration", cancellationToken);
        var expected = TypedValueFactory.CreateProfile("remove-expiration");
        await cache.SetAsync("k", expected, Expiry.In(TimeSpan.FromMinutes(5)), cancellationToken);
        _ = await Assert.That(await cache.RemoveExpirationAsync("k", cancellationToken)).IsTrue();
        var expiration = await cache.GetExpirationAsync("k", cancellationToken);
        var result = await cache.GetValueAsync("k", cancellationToken);
        _ = await Assert.That(result.Found).IsTrue();
        _ = await Assert.That(expiration.HasExpiration).IsFalse();
        await TypedValueAssertions.AssertProfileEquals(expected, result.Value!);
    }

    /// <summary>Verifies TouchUpdatesCustomRecordExpirySingleNode.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchUpdatesCustomRecordExpirySingleNode(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-touch", cancellationToken);
        var expected = TypedValueFactory.CreateProfile("touch");
        await cache.SetAsync("k", expected, cancellationToken: cancellationToken);
        _ = await Assert.That(await cache.TouchAsync("k", TimeSpan.FromMinutes(2), cancellationToken)).IsTrue();
        var expiration = await cache.GetExpirationAsync("k", cancellationToken);
        var result = await cache.GetValueAsync("k", cancellationToken);
        _ = await Assert.That(expiration.Found).IsTrue();
        _ = await Assert.That(expiration.HasExpiration).IsTrue();
        _ = await Assert.That(expiration.Expiration > TimeSpan.Zero).IsTrue();
        _ = await Assert.That(result.Found).IsTrue();
        await TypedValueAssertions.AssertProfileEquals(expected, result.Value!);
    }

    /// <summary>Verifies UpdatePreservesCustomRecordExpiry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UpdatePreservesCustomRecordExpiry(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<TypedCustomerProfile>("typed-single-update", cancellationToken);
        var updated = TypedValueFactory.CreateUpdatedProfile("update");
        await cache.SetAsync("k", TypedValueFactory.CreateProfile("update"), Expiry.In(TimeSpan.FromMinutes(5)), cancellationToken);
        _ = await Assert.That(await cache.UpdateAsync("k", updated, cancellationToken)).IsTrue();
        var result = await cache.GetValueAsync("k", cancellationToken);
        var expiration = await cache.GetExpirationAsync("k", cancellationToken);
        _ = await Assert.That(result.Found).IsTrue();
        _ = await Assert.That(expiration.HasExpiration).IsTrue();
        await TypedValueAssertions.AssertProfileEquals(updated, result.Value!);
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
