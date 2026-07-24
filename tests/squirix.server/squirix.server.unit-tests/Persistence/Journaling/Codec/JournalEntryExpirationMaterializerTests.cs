using System;
using System.Globalization;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Codec;

/// <summary>Unit tests for <see cref="JournalEntryExpirationMaterializer" />.</summary>
public sealed class JournalEntryExpirationMaterializerTests
{
    /// <summary>Verifies relative TTL is converted to absolute expiry before journal write.</summary>
    [Fact]
    public void ForJournalWriteMaterializesExpirationExpiresUtc()
    {
        var before = DateTime.UtcNow;
        var (expiresUtc, expiration) = JournalEntryExpirationMaterializer.ForJournalWrite(null, TimeSpan.FromMilliseconds(100));
        var after = DateTime.UtcNow.Add(TimeSpan.FromMilliseconds(100));

        Assert.Null(expiration);
        Assert.True(expiresUtc is not null);
        Assert.InRange(expiresUtc.Value, before, after);
    }

    /// <summary>Verifies replay skips relative TTL entries using the journal record timestamp.</summary>
    [Fact]
    public void IsExpiredRecoveryUsesTimestampRelativeExpiration()
    {
        var writtenUnixMs = DateTimeOffset.UtcNow.AddSeconds(-1).ToUnixTimeMilliseconds();

        Assert.True(JournalEntryExpirationMaterializer.IsExpiredForRecovery(null, TimeSpan.FromMilliseconds(100), writtenUnixMs));
        Assert.False(JournalEntryExpirationMaterializer.IsExpiredForRecovery(null, TimeSpan.FromMinutes(5), writtenUnixMs));
    }

    /// <summary>Verifies recovery insert converts legacy relative TTL payloads to absolute expiry.</summary>
    [Fact]
    public void ForRecoveryInsertMaterializesExpiryRecordTimestamp()
    {
        var writtenUnixMs = DateTimeOffset.Parse("2020-01-01T00:00:00Z", CultureInfo.InvariantCulture).ToUnixTimeMilliseconds();
        var entry = new NodeCacheEntry<string> { Value = "v", Expiration = TimeSpan.FromSeconds(30) };

        var restored = JournalEntryExpirationMaterializer.ForRecoveryInsert(entry, writtenUnixMs);

        Assert.Null(restored.Expiration);
        var memory = DateTime.Parse("2020-01-01T00:00:30Z", CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        Assert.Equal(memory, restored.ExpiresUtc);
    }
}
