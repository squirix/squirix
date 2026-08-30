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

    /// <summary>ForJournalWrite keeps the earliest of the relative and absolute deadlines (issue #445).</summary>
    [Fact]
    public void WriteMaterializesEarliestDeadline()
    {
        var start = DateTime.UtcNow;

        var (relativeDeadline, relativeExpiration) = JournalEntryExpirationMaterializer.ForJournalWrite(start.AddHours(1), TimeSpan.FromMilliseconds(100));
        Assert.Null(relativeExpiration);
        _ = Assert.NotNull(relativeDeadline);
        Assert.InRange(relativeDeadline.Value, start.AddMilliseconds(100), start.AddSeconds(1));

        var (absoluteDeadline, _) = JournalEntryExpirationMaterializer.ForJournalWrite(start.AddMilliseconds(-1000), TimeSpan.FromMinutes(5));
        Assert.Equal(start.AddMilliseconds(-1000), absoluteDeadline);
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

    /// <summary>Recovery insert uses the earliest of the absolute and relative deadlines (issue #445).</summary>
    [Fact]
    public void RecoveryInsertUsesEarliestDeadline()
    {
        var writtenUnixMs = DateTimeOffset.Parse("2020-01-01T00:00:00Z", CultureInfo.InvariantCulture).ToUnixTimeMilliseconds();
        var write = DateTime.Parse("2020-01-01T00:00:00Z", CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);

        var relativeWins = new NodeCacheEntry<string> { Value = "v", Expiration = TimeSpan.FromSeconds(30), ExpiresUtc = write.AddMinutes(5) };
        var restoredRelative = JournalEntryExpirationMaterializer.ForRecoveryInsert(relativeWins, writtenUnixMs);

        Assert.Null(restoredRelative.Expiration);
        Assert.Equal(write.AddSeconds(30), restoredRelative.ExpiresUtc);

        var absoluteWins = new NodeCacheEntry<string> { Value = "v", Expiration = TimeSpan.FromMinutes(5), ExpiresUtc = write.AddSeconds(10) };
        var restoredAbsolute = JournalEntryExpirationMaterializer.ForRecoveryInsert(absoluteWins, writtenUnixMs);

        Assert.Equal(write.AddSeconds(10), restoredAbsolute.ExpiresUtc);
    }

    /// <summary>Verifies replay skips relative TTL entries using the journal record timestamp.</summary>
    [Fact]
    public void IsExpiredUsesRelativeRecoveryTimestamp()
    {
        var writtenUnixMs = DateTimeOffset.UtcNow.AddSeconds(-1).ToUnixTimeMilliseconds();

        Assert.True(JournalEntryExpirationMaterializer.IsExpiredForRecovery(null, TimeSpan.FromMilliseconds(100), writtenUnixMs));
        Assert.False(JournalEntryExpirationMaterializer.IsExpiredForRecovery(null, TimeSpan.FromMinutes(5), writtenUnixMs));
    }

    /// <summary>ForJournalWrite saturates a relative deadline exceeding the DateTime range instead of throwing.</summary>
    [Fact]
    public void WriteMaterializesSaturatedDeadline()
    {
        var (expiresUtc, expiration) = JournalEntryExpirationMaterializer.ForJournalWrite(null, TimeSpan.MaxValue);

        Assert.Null(expiration);
        Assert.Equal(DateTime.MaxValue, expiresUtc);
    }

    /// <summary>Recovery insert saturates a relative deadline exceeding the DateTime range instead of throwing.</summary>
    [Fact]
    public void RecoveryInsertSaturatesHugeDeadline()
    {
        var entry = new NodeCacheEntry<string> { Value = "v", Expiration = TimeSpan.MaxValue };

        var restored = JournalEntryExpirationMaterializer.ForRecoveryInsert(entry, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        Assert.Null(restored.Expiration);
        Assert.Equal(DateTime.MaxValue, restored.ExpiresUtc);
    }

    /// <summary>Recovery expiry check treats a saturated relative deadline as not expired instead of throwing.</summary>
    [Fact]
    public void RecoveryNotExpiredForSaturatedDeadline()
    {
        var writtenUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        Assert.False(JournalEntryExpirationMaterializer.IsExpiredForRecovery(null, TimeSpan.MaxValue, writtenUnixMs));
    }
}
