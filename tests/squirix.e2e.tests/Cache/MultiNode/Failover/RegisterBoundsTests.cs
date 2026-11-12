using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using Squirix.E2ETests.Fixtures;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode.Failover;

/// <summary>Checks indexed history bounds against a linear oracle without introducing read-to-read ordering.</summary>
public sealed class RegisterBoundsTests
{
    /// <summary>Arbitrary input order, gaps in values, ambiguous writes and time boundaries preserve the linear verdict and diagnostics.</summary>
    /// <param name="writeCount">The number of sequential writes in each history.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    [Arguments(4)]
    [Arguments(5)]
    public async Task ShuffledHistoriesMatchLinearBounds(int writeCount)
    {
        var writes = new RegisterWrite[writeCount];
        var reads = OverlappingReads();
        for (var mask = 0; mask < 1 << writeCount; mask++)
        {
            for (var i = 0; i < writes.Length; i++)
                writes[writes.Length - i - 1] = new RegisterWrite("k", (i * 3) + 2, i * 10, (i * 10) + 3, (mask & (1 << i)) != 0);

            var expected = LinearBounds(writes, reads);
            var actual = RegisterHistory.Check(writes, reads);

            _ = await Assert.That(actual.Count).IsEqualTo(expected.Count).Because($"writes={writeCount}, acknowledgements={mask}");
            for (var i = 0; i < expected.Count; i++)
                _ = await Assert.That(actual[i]).IsEqualTo(expected[i]).Because($"writes={writeCount}, acknowledgements={mask}, violation={i}");
        }
    }

    /// <summary>Bounds diagnostics follow input read order even when writes and reads arrive out of timestamp order.</summary>
    [Test]
    public async Task BoundsMessagesKeepInputReadOrder()
    {
        RegisterWrite[] writes = [new("k", 9, 20, 25, true), new("k", 1, 1, 5, true), new("k", 4, 6, 7, false)];
        RegisterRead[] reads = [new("k", 30, 100, 0), new("k", 10, 100, 0), new("k", 15, 100, 7), new("k", 26, 100, 9), new("k", 27, 100, 4)];

        var violations = RegisterHistory.Check(writes, reads);

        _ = await Assert.That(violations.Count).IsEqualTo(3);
        _ = await Assert.That(violations[0]).IsEqualTo("key k: read at [30, 100] saw 0, a value below 9, acknowledged before the read started, so an acknowledged write is lost");
        _ = await Assert.That(violations[1]).IsEqualTo("key k: read at [10, 100] saw 0, a value below 1, acknowledged before the read started, so an acknowledged write is lost");
        _ = await Assert.That(violations[2]).IsEqualTo("key k: read at [15, 100] saw 7, a value no write wrote");
    }

    /// <summary>All writes ending exactly at a read's start still overlap it; one tick later the highest acknowledged value is required.</summary>
    [Test]
    public async Task TiedWriteEndsUseStrictReadBoundary()
    {
        RegisterWrite[] writes = [new("k", 1, 10, 20, true), new("k", 9, 20, 20, true)];
        RegisterRead[] reads = [new("k", 20, 30, 0), new("k", 21, 30, 0)];

        var violations = RegisterHistory.Check(writes, reads);

        _ = await Assert.That(violations).HasSingleItem();
        _ = await Assert.That(violations[0]).IsEqualTo("key k: read at [21, 30] saw 0, a value below 9, acknowledged before the read started, so an acknowledged write is lost");
    }

    private static List<string> LinearBounds(RegisterWrite[] writes, List<RegisterRead> reads)
    {
        var violations = new List<string>();
        for (var i = 0; i < reads.Count; i++)
        {
            var read = reads[i];
            var reason = LinearReadViolation(writes, in read);
            if (reason != null)
                violations.Add(string.Create(CultureInfo.InvariantCulture, $"key k: read at [{read.Start}, {read.End}] saw {read.Observed}, {reason}"));
        }

        return violations;
    }

    private static string? LinearReadViolation(RegisterWrite[] writes, in RegisterRead read)
    {
        var found = read.Observed == 0;
        var started = long.MinValue;
        var acknowledged = false;
        var highest = 0L;
        foreach (var write in writes)
        {
            if (write.Value == read.Observed)
            {
                found = true;
                started = write.Start;
                acknowledged = write.Acked;
            }

            if (write.Acked && write.End < read.Start)
                highest = Math.Max(highest, write.Value);
        }

        return true switch
        {
            _ when !found => "a value no write wrote",
            _ when started > read.End => "a value whose write started after the read returned",
            _ when (read.Observed == 0 || acknowledged) && read.Observed < highest =>
                $"a value below {highest}, acknowledged before the read started, so an acknowledged write is lost",
            _ => null,
        };
    }

    private static List<RegisterRead> OverlappingReads()
    {
        long[] starts = [20, 14, 13, 4, 3, -1];
        long[] ends = [20, 23, 30, 40, 41, 50];
        long[] observed = [long.MinValue, -1, 0, 1, 2, 5, 8, 11, 14, long.MaxValue];
        var reads = new List<RegisterRead>();
        foreach (var start in starts)
            foreach (var end in ends)
                foreach (var value in observed)
                    reads.Add(new RegisterRead("k", start, end, value));

        return reads;
    }
}
