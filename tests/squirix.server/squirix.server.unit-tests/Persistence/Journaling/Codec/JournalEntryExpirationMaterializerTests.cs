using System;
using System.Globalization;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Codec;

/// <summary>Unit tests for <see cref="JournalEntryExpirationMaterializer" />.</summary>
[Immutable]
public sealed class JournalEntryExpirationMaterializerTests
{
    /// <summary>Verifies replay skips relative TTL entries using the journal record timestamp.</summary>
    [Test]
    public async Task IsExpiredUsesRelativeRecoveryTimestamp()
    {
        var writtenUnixMs = DateTimeOffset.UtcNow.AddSeconds(-1).ToUnixTimeMilliseconds();

        _ = await Assert.That(JournalEntryExpirationMaterializer.IsExpiredForRecovery(null, TimeSpan.FromMilliseconds(100), writtenUnixMs)).IsTrue();
        _ = await Assert.That(JournalEntryExpirationMaterializer.IsExpiredForRecovery(null, TimeSpan.FromMinutes(5), writtenUnixMs)).IsFalse();
    }

    /// <summary>Recovery insert saturates a relative deadline exceeding the DateTime range instead of throwing.</summary>
    [Test]
    public async Task RecoveryInsertSaturatesHugeDeadline()
    {
        var entry = new NodeCacheEntry<string> { Value = "v", Expiration = TimeSpan.MaxValue };

        var restored = JournalEntryExpirationMaterializer.ForRecoveryInsert(entry, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        _ = await Assert.That(restored.Expiration).IsNull();
        _ = await Assert.That(restored.ExpiresUtc).IsEqualTo(DateTime.MaxValue);
    }

    /// <summary>Verifies recovery insert converts legacy relative TTL payloads to absolute expiry.</summary>
    [Test]
    public async Task RecoveryInsertSetsExpiryFromTimestamp()
    {
        var writtenUnixMs = DateTimeOffset.Parse("2020-01-01T00:00:00Z", CultureInfo.InvariantCulture).ToUnixTimeMilliseconds();
        var entry = new NodeCacheEntry<string> { Value = "v", Expiration = TimeSpan.FromSeconds(30) };

        var restored = JournalEntryExpirationMaterializer.ForRecoveryInsert(entry, writtenUnixMs);

        _ = await Assert.That(restored.Expiration).IsNull();
        var memory = DateTime.Parse("2020-01-01T00:00:30Z", CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        _ = await Assert.That(restored.ExpiresUtc).IsEqualTo(memory);
    }

    /// <summary>Recovery insert uses the earliest of the absolute and relative deadlines (issue #445).</summary>
    [Test]
    public async Task RecoveryInsertUsesEarliestDeadline()
    {
        var writtenUnixMs = DateTimeOffset.Parse("2020-01-01T00:00:00Z", CultureInfo.InvariantCulture).ToUnixTimeMilliseconds();
        var write = DateTime.Parse("2020-01-01T00:00:00Z", CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);

        var relativeWins = new NodeCacheEntry<string> { Value = "v", Expiration = TimeSpan.FromSeconds(30), ExpiresUtc = write.AddMinutes(5) };
        var restoredRelative = JournalEntryExpirationMaterializer.ForRecoveryInsert(relativeWins, writtenUnixMs);

        _ = await Assert.That(restoredRelative.Expiration).IsNull();
        _ = await Assert.That(restoredRelative.ExpiresUtc).IsEqualTo(write.AddSeconds(30));

        var absoluteWins = new NodeCacheEntry<string> { Value = "v", Expiration = TimeSpan.FromMinutes(5), ExpiresUtc = write.AddSeconds(10) };
        var restoredAbsolute = JournalEntryExpirationMaterializer.ForRecoveryInsert(absoluteWins, writtenUnixMs);

        _ = await Assert.That(restoredAbsolute.ExpiresUtc).IsEqualTo(write.AddSeconds(10));
    }

    /// <summary>Recovery expiry check treats a saturated relative deadline as not expired instead of throwing.</summary>
    [Test]
    public async Task RecoveryNotExpiredForSaturatedDeadline()
    {
        var writtenUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        _ = await Assert.That(JournalEntryExpirationMaterializer.IsExpiredForRecovery(null, TimeSpan.MaxValue, writtenUnixMs)).IsFalse();
    }

    /// <summary>ForJournalWrite keeps the earliest of the relative and absolute deadlines (issue #445).</summary>
    [Test]
    public async Task WriteMaterializesEarliestDeadline()
    {
        var start = DateTime.UtcNow;

        var (relativeDeadline, relativeExpiration) = JournalEntryExpirationMaterializer.ForJournalWrite(start.AddHours(1), TimeSpan.FromMilliseconds(100));
        _ = await Assert.That(relativeExpiration).IsNull();
        _ = await Assert.That(relativeDeadline).IsNotNull();
        _ = await Assert.That(relativeDeadline.Value).IsBetween(start.AddMilliseconds(100), start.AddSeconds(1));

        var (absoluteDeadline, _) = JournalEntryExpirationMaterializer.ForJournalWrite(start.AddMilliseconds(-1000), TimeSpan.FromMinutes(5));
        _ = await Assert.That(absoluteDeadline).IsEqualTo(start.AddMilliseconds(-1000));
    }

    /// <summary>Verifies relative TTL is converted to absolute expiry before journal write.</summary>
    [Test]
    public async Task WriteMaterializesExpiresUtcExpiry()
    {
        var before = DateTime.UtcNow;
        var (expiresUtc, expiration) = JournalEntryExpirationMaterializer.ForJournalWrite(null, TimeSpan.FromMilliseconds(100));
        var after = DateTime.UtcNow.Add(TimeSpan.FromMilliseconds(100));

        _ = await Assert.That(expiration).IsNull();
        _ = await Assert.That(expiresUtc).IsNotNull();
        _ = await Assert.That(expiresUtc.Value).IsBetween(before, after);
    }

    /// <summary>ForJournalWrite saturates a relative deadline exceeding the DateTime range instead of throwing.</summary>
    [Test]
    public async Task WriteMaterializesSaturatedDeadline()
    {
        var (expiresUtc, expiration) = JournalEntryExpirationMaterializer.ForJournalWrite(null, TimeSpan.MaxValue);

        _ = await Assert.That(expiration).IsNull();
        _ = await Assert.That(expiresUtc).IsEqualTo(DateTime.MaxValue);
    }
}
