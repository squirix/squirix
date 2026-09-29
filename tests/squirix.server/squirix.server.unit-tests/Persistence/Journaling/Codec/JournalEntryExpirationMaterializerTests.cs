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
    private static readonly DateTime Boundary = new(2026, 1, 1, 0, 0, 0, 5, DateTimeKind.Utc);

    /// <summary>A deadline on a millisecond boundary is unchanged and no deadline stays none.</summary>
    [Test]
    public async Task PinKeepsBoundaryAndNone()
    {
        _ = await Assert.That(JournalEntryExpirationMaterializer.PinToJournalPrecision(Boundary)).IsEqualTo(Boundary);
        _ = await Assert.That(JournalEntryExpirationMaterializer.PinToJournalPrecision(null)).IsNull();
    }

    /// <summary>A deadline between milliseconds rounds up so the entry never lives shorter than requested.</summary>
    [Test]
    public async Task PinRoundsUpToNextMillisecond()
    {
        var pinned = JournalEntryExpirationMaterializer.PinToJournalPrecision(Boundary.AddTicks(1));

        _ = await Assert.That(pinned).IsEqualTo(Boundary.AddMilliseconds(1));
    }

    /// <summary>A deadline near the largest date clamps to the largest whole millisecond instead of overflowing.</summary>
    [Test]
    public async Task PinClampsAtLargestWholeMillisecond()
    {
        var pinned = JournalEntryExpirationMaterializer.PinToJournalPrecision(DateTime.MaxValue);

        _ = await Assert.That(pinned!.Value.Ticks % TimeSpan.TicksPerMillisecond).IsEqualTo(0L);
        _ = await Assert.That(pinned.Value <= DateTime.MaxValue).IsTrue();
    }

    /// <summary>A durable write pins a relative deadline and an absolute-only deadline alike.</summary>
    [Test]
    public async Task DurableWritePinsEffectiveDeadline()
    {
        var now = Boundary.AddTicks(3);
        var relative = JournalEntryExpirationMaterializer.ForDurableWrite(new NodeCacheEntry<string>("v", expiration: TimeSpan.FromSeconds(1)), now);
        var absolute = JournalEntryExpirationMaterializer.ForDurableWrite(new NodeCacheEntry<string>("v", expiresUtc: now.AddSeconds(1)), now);

        _ = await Assert.That(relative.ExpiresUtc).IsEqualTo(Boundary.AddSeconds(1).AddMilliseconds(1));
        _ = await Assert.That(absolute.ExpiresUtc).IsEqualTo(Boundary.AddSeconds(1).AddMilliseconds(1));
        _ = await Assert.That(relative.Expiration).IsNull();
    }

    /// <summary>ForDurableWrite resolves a relative TTL against the given instant and keeps the earliest deadline.</summary>
    [Test]
    public async Task DurableWriteUsesEarliestDeadline()
    {
        var write = DateTime.Parse("2020-01-01T00:00:00Z", CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);

        var relativeWins = new NodeCacheEntry<string> { Value = "v", Expiration = TimeSpan.FromSeconds(30), ExpiresUtc = write.AddMinutes(5) };
        var resolvedRelative = JournalEntryExpirationMaterializer.ForDurableWrite(relativeWins, write);

        _ = await Assert.That(resolvedRelative.Expiration).IsNull();
        _ = await Assert.That(resolvedRelative.ExpiresUtc).IsEqualTo(write.AddSeconds(30));

        var absoluteWins = new NodeCacheEntry<string> { Value = "v", Expiration = TimeSpan.FromMinutes(5), ExpiresUtc = write.AddSeconds(10) };
        var resolvedAbsolute = JournalEntryExpirationMaterializer.ForDurableWrite(absoluteWins, write);

        _ = await Assert.That(resolvedAbsolute.Expiration).IsNull();
        _ = await Assert.That(resolvedAbsolute.ExpiresUtc).IsEqualTo(write.AddSeconds(10));

        var absoluteOnly = new NodeCacheEntry<string> { Value = "v", ExpiresUtc = write.AddSeconds(10) };
        _ = await Assert.That(JournalEntryExpirationMaterializer.ForDurableWrite(absoluteOnly, write)).IsSameReferenceAs(absoluteOnly);
    }

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

    /// <summary>Recovery insert uses the earliest of the absolute and relative deadlines.</summary>
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

    /// <summary>ForJournalWrite keeps the earliest of the relative and absolute deadlines.</summary>
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
