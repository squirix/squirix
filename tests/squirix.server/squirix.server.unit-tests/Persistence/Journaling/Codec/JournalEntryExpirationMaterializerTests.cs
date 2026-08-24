using System;
using System.Globalization;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Codec;

/// <summary>Unit tests for <see cref="JournalEntryExpirationMaterializer" />.</summary>
[Immutable]
public sealed class JournalEntryExpirationMaterializerTests
{
    /// <summary>Verifies relative TTL is converted to absolute expiry before journal write.</summary>
    [Fact]
    public void WriteMaterializesExpiresUtcExpiry()
    {
        var before = DateTime.UtcNow;
        var (expiresUtc, expiration) = JournalEntryExpirationMaterializer.ForJournalWrite(null, TimeSpan.FromMilliseconds(100));
        var after = DateTime.UtcNow.Add(TimeSpan.FromMilliseconds(100));

        Assert.Null(expiration);
        _ = Assert.NotNull(expiresUtc);
        Assert.InRange(expiresUtc.Value, before, after);
    }

    /// <summary>Verifies recovery insert converts legacy relative TTL payloads to absolute expiry.</summary>
    [Fact]
    public void RecoveryInsertSetsExpiryFromTimestamp()
    {
        var writtenUnixMs = DateTimeOffset.Parse("2020-01-01T00:00:00Z", CultureInfo.InvariantCulture).ToUnixTimeMilliseconds();
        var entry = new NodeCacheEntry<string> { Value = "v", Expiration = TimeSpan.FromSeconds(30) };

        var restored = JournalEntryExpirationMaterializer.ForRecoveryInsert(entry, writtenUnixMs);

        Assert.Null(restored.Expiration);
        var memory = DateTime.Parse("2020-01-01T00:00:30Z", CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        Assert.Equal(memory, restored.ExpiresUtc);
    }

    /// <summary>Verifies replay skips relative TTL entries using the journal record timestamp.</summary>
    [Fact]
    public void IsExpiredUsesRelativeRecoveryTimestamp()
    {
        var writtenUnixMs = DateTimeOffset.UtcNow.AddSeconds(-1).ToUnixTimeMilliseconds();

        Assert.True(JournalEntryExpirationMaterializer.IsExpiredForRecovery(null, TimeSpan.FromMilliseconds(100), writtenUnixMs));
        Assert.False(JournalEntryExpirationMaterializer.IsExpiredForRecovery(null, TimeSpan.FromMinutes(5), writtenUnixMs));
    }
}
