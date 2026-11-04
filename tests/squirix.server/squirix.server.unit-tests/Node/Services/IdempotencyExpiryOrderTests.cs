using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Services;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Local expiry avoids the wall heap only after both clocks pass retention.</summary>
[Immutable]
public sealed class IdempotencyExpiryOrderTests
{
    /// <summary>Equality on either clock is retained; a wall-delayed record remains available for a later sweep.</summary>
    /// <param name="monotonicSeconds">Elapsed monotonic seconds.</param>
    /// <param name="wallSeconds">Elapsed wall seconds.</param>
    /// <param name="expected">Whether both clocks permit expiry.</param>
    [Test]
    [Arguments(10, 11, false)]
    [Arguments(11, 10, false)]
    [Arguments(11, -1, false)]
    [Arguments(11, 11, true)]
    public async Task ExpiryRequiresBothClocks(int monotonicSeconds, int wallSeconds, bool expected)
    {
        var clock = new FakeTimeProvider();
        var created = clock.GetUtcNow().UtcDateTime;
        var order = new IdempotencyExpiryOrder(clock, (string key, long sequence, out DateTime date) =>
        {
            date = created;
            return string.Equals(key, "a", StringComparison.Ordinal) && sequence == 1;
        });
        order.Add("a", 1, clock.GetTimestamp(), created, false);
        clock.Advance(TimeSpan.FromSeconds(monotonicSeconds));
        var expired = order.TryTakeExpired(TimeSpan.FromSeconds(10), created.AddSeconds(wallSeconds), clock.GetTimestamp(), out var key);

        _ = await Assert.That(expired).IsEqualTo(expected);
        _ = await Assert.That(key).IsEqualTo(expected ? "a" : string.Empty);
        if (!expected)
        {
            clock.Advance(TimeSpan.FromSeconds(20));
            _ = await Assert.That(order.TryTakeExpired(TimeSpan.FromSeconds(10), created.AddSeconds(30), clock.GetTimestamp(), out var later)).IsTrue();
            _ = await Assert.That(later).IsEqualTo("a");
        }

        _ = await Assert.That(order.EntryCount).IsEqualTo(0);
    }

    /// <summary>Taking one expired local record does not look up or enqueue all remaining local records.</summary>
    [Test]
    public async Task ExpiredLocalRecordReturnsImmediately()
    {
        var clock = new FakeTimeProvider();
        var created = clock.GetUtcNow().UtcDateTime;
        var lookups = 0;
        var order = new IdempotencyExpiryOrder(clock, (string _, long _, out DateTime date) =>
        {
            lookups++;
            date = created;
            return true;
        });
        order.Add("a", 1, clock.GetTimestamp(), created, false);
        order.Add("b", 2, clock.GetTimestamp(), created, false);
        clock.Advance(TimeSpan.FromSeconds(11));

        var expired = order.TryTakeExpired(TimeSpan.FromSeconds(10), clock.GetUtcNow().UtcDateTime, clock.GetTimestamp(), out var key);

        _ = await Assert.That((expired, key, lookups, order.EntryCount)).IsEqualTo((true, "a", 1, 1));
    }
}
