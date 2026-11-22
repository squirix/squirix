using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Fixtures;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode.Failover;

/// <summary>Checks the search against independently enumerated sequential executions of small histories.</summary>
public sealed class RegisterSearchOracleTests
{
    /// <summary>Long histories use a heap-backed search stack rather than exhausting the process call stack.</summary>
    [Test]
    public async Task LongHistoryDoesNotExhaustCallStack()
    {
        var writes = new RegisterWrite[10_000];
        var reads = new RegisterRead[10_000];
        for (var i = 0; i < writes.Length; i++)
        {
            writes[i] = new RegisterWrite("k", i + 1, i * 4, (i * 4) + 1, true);
            reads[i] = new RegisterRead("k", (i * 4) + 2, (i * 4) + 3, i + 1);
        }

        _ = await Assert.That(RegisterLinearizability.Check(writes, reads, CancellationToken.None)).IsEmpty();
    }

    /// <summary>Every observation triple is checked with overlapping and disjoint read intervals.</summary>
    /// <param name="overlap">Whether the reads overlap each other and the final write.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task AgreesWithPermutationOracle(bool overlap)
    {
        RegisterWrite[] writes = [new("k", 1, 1, 2, true), new("k", 2, 3, 4, false), new("k", 3, 5, 8, true)];
        for (var observations = 0; observations < 64; observations++)
        {
            var first = observations / 16;
            var second = (observations / 4) % 4;
            var third = observations % 4;
            RegisterRead[] reads = overlap
                ? [new RegisterRead("k", 6, 12, first), new RegisterRead("k", 7, 11, second), new RegisterRead("k", 8, 10, third)]
                : [new RegisterRead("k", 9, 10, first), new RegisterRead("k", 11, 12, second), new RegisterRead("k", 13, 14, third)];
            var expected = Permits(writes, reads, false) || Permits(writes, reads, true);
            var actual = RegisterLinearizability.Check(writes, reads, CancellationToken.None).Count == 0;
            _ = await Assert.That(actual).IsEqualTo(expected).Because($"observations {first}, {second}, {third}; overlap={overlap}");
        }
    }

    private static bool Permits(RegisterWrite[] writes, RegisterRead[] reads, bool includeAmbiguous)
    {
        var operations = new List<(long Start, long End, long Value, bool Read)>();
        foreach (var write in writes)
        {
            if (write.Acked || includeAmbiguous)
                operations.Add((write.Start, write.Acked ? write.End : long.MaxValue, write.Value, false));
        }

        foreach (var read in reads)
            operations.Add((read.Start, read.End, read.Observed, true));
        return Permute([.. operations], 0);
    }

    private static bool Permute((long Start, long End, long Value, bool Read)[] operations, int index)
    {
        if (index == operations.Length)
            return IsLegal(operations);
        for (var i = index; i < operations.Length; i++)
        {
            (operations[index], operations[i]) = (operations[i], operations[index]);
            var legal = Permute(operations, index + 1);
            (operations[index], operations[i]) = (operations[i], operations[index]);
            if (legal)
                return true;
        }

        return false;
    }

    private static bool IsLegal((long Start, long End, long Value, bool Read)[] operations)
    {
        var value = 0L;
        var timestamp = long.MinValue;
        foreach (var (start, end, observed, read) in operations)
        {
            timestamp = Math.Max(timestamp, start);
            if (timestamp > end)
                return false;
            if (read && observed != value)
                return false;
            value = read ? value : observed;
        }

        return true;
    }
}
