using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Threading;

namespace Squirix.E2ETests.Fixtures;

/// <summary>
/// Records the calls on a set of single-writer registers and checks that every read is linearizable: one cache key per register, written
/// by one writer with increasing positive integers.
/// </summary>
/// <remarks>
/// <para>
/// With one writer and unique increasing values, a history is linearizable exactly when every successful read of a key satisfies four
/// rules:
/// </para>
/// <list type="bullet">
///   <item>it sees a value some write of the key wrote, or zero (not found) for the register before its first write;</item>
///   <item>
///     it sees no value below the highest acknowledged write that returned before the read started, so no acknowledged write is lost
///     and no read is stale;
///   </item>
///   <item>it sees no value above the highest write that started before the read returned, so no read sees the future;</item>
///   <item>it sees no value below one a read that returned before it started saw, so reads never go back in time.</item>
/// </list>
/// <para>
/// A failed write is ambiguous: it may or may not have taken effect, so a read may see its value but no read has to. Failed reads impose
/// nothing and are only counted. Recording is thread safe; <see cref="Check(IReadOnlyList{RegisterWrite}, IReadOnlyList{RegisterRead})" />
/// is a pure function of the records.
/// </para>
/// </remarks>
internal sealed class RegisterHistory
{
    private readonly Lock _gate = new();
    private readonly List<RegisterRead> _reads = [];
    private readonly List<RegisterWrite> _writes = [];
    private int _ambiguousWrites;
    private int _failedReads;

    /// <summary>Gets the number of writes that failed, whose effect is ambiguous.</summary>
    internal int AmbiguousWrites
    {
        get
        {
            lock (_gate)
                return _ambiguousWrites;
        }
    }

    /// <summary>Gets the number of reads that failed.</summary>
    internal int FailedReads
    {
        get
        {
            lock (_gate)
                return _failedReads;
        }
    }

    /// <summary>Checks a history and returns every violation found, in key order; an empty list means the history is linearizable.</summary>
    /// <param name="writes">The writes, in any order.</param>
    /// <param name="reads">The successful reads, in any order.</param>
    /// <returns>One line per violation.</returns>
    internal static List<string> Check(IReadOnlyList<RegisterWrite> writes, IReadOnlyList<RegisterRead> reads)
    {
        ArgumentNullException.ThrowIfNull(writes);
        ArgumentNullException.ThrowIfNull(reads);
        var writesByKey = GroupWrites(writes);
        var readsByKey = GroupReads(reads);
        var keys = new SortedSet<string>(writesByKey.Keys, StringComparer.Ordinal);
        keys.UnionWith(readsByKey.Keys);

        var violations = new List<string>();
        foreach (var key in keys)
        {
            var keyWrites = writesByKey.TryGetValue(key, out var w) ? w : [];
            var keyReads = readsByKey.TryGetValue(key, out var r) ? r : [];
            if (CheckWriter(key, keyWrites, violations))
            {
                CheckBounds(key, keyWrites, keyReads, violations);
                CheckMonotonic(key, keyReads, violations);
            }
        }

        return violations;
    }

    /// <summary>Checks the recorded history.</summary>
    /// <returns>One line per violation; empty when the history is linearizable.</returns>
    internal List<string> Check()
    {
        RegisterWrite[] writes;
        RegisterRead[] reads;
        lock (_gate)
        {
            writes = [.. _writes];
            reads = [.. _reads];
        }

        return Check(writes, reads);
    }

    /// <summary>Records a read that failed; it constrains nothing.</summary>
    internal void RecordFailedRead()
    {
        lock (_gate)
            _failedReads++;
    }

    /// <summary>Records a successful read.</summary>
    /// <param name="read">The read.</param>
    internal void RecordRead(in RegisterRead read)
    {
        lock (_gate)
            _reads.Add(read);
    }

    /// <summary>Records a write, acknowledged or failed.</summary>
    /// <param name="write">The write.</param>
    internal void RecordWrite(in RegisterWrite write)
    {
        lock (_gate)
        {
            _writes.Add(write);
            if (!write.Acked)
                _ambiguousWrites++;
        }
    }

    /// <summary>Describes the recorded history in one line, for assertion messages.</summary>
    /// <returns>The counts of acknowledged and failed writes, and of successful and failed reads.</returns>
    internal string Summary()
    {
        lock (_gate)
        {
            return string.Create(
                CultureInfo.InvariantCulture,
                $"{_writes.Count - _ambiguousWrites} acknowledged and {_ambiguousWrites} failed writes, {_reads.Count} successful and {_failedReads} failed reads");
        }
    }

    /// <summary>Checks each read of a key against the writes: a written value, not below the acknowledged past, not above the started writes.</summary>
    /// <param name="key">The register key.</param>
    /// <param name="writes">The writes of the key.</param>
    /// <param name="reads">The reads of the key.</param>
    /// <param name="violations">Receives one line per violation.</param>
    private static void CheckBounds(string key, List<RegisterWrite> writes, List<RegisterRead> reads, List<string> violations)
    {
        var keyWrites = CollectionsMarshal.AsSpan(writes);
        foreach (ref readonly var read in CollectionsMarshal.AsSpan(reads))
        {
            var written = read.Observed == 0;
            var ackedBefore = 0L;
            var startedBefore = 0L;
            foreach (ref readonly var write in keyWrites)
            {
                written |= write.Value == read.Observed;
                if (write.Acked && write.End < read.Start)
                    ackedBefore = Math.Max(ackedBefore, write.Value);

                if (write.Start <= read.End)
                    startedBefore = Math.Max(startedBefore, write.Value);
            }

            if (!written)
                violations.Add(Describe(key, in read, "a value no write wrote"));
            else if (read.Observed < ackedBefore)
                violations.Add(Describe(key, in read, $"a value below {ackedBefore}, acknowledged before the read started"));
            else if (read.Observed > startedBefore)
                violations.Add(Describe(key, in read, $"a value above {startedBefore}, the last write started before the read returned"));
        }
    }

    /// <summary>Checks that no read of a key sees a value below one a read that returned before it started saw.</summary>
    /// <param name="key">The register key.</param>
    /// <param name="reads">The reads of the key.</param>
    /// <param name="violations">Receives one line per violation.</param>
    private static void CheckMonotonic(string key, List<RegisterRead> reads, List<string> violations)
    {
        RegisterRead[] byStart = [.. reads];
        RegisterRead[] byEnd = [.. reads];
        Array.Sort(byStart, static (a, b) => a.Start.CompareTo(b.Start));
        Array.Sort(byEnd, static (a, b) => a.End.CompareTo(b.End));

        // Sweep the reads in start order; the reads that returned before the current one started form a growing prefix of the end order.
        var returned = 0;
        var highest = (Value: 0L, Read: default(RegisterRead));
        foreach (ref readonly var read in byStart.AsSpan())
        {
            while (returned < byEnd.Length && byEnd[returned].End < read.Start)
            {
                if (byEnd[returned].Observed > highest.Value)
                    highest = (byEnd[returned].Observed, byEnd[returned]);

                returned++;
            }

            if (read.Observed < highest.Value)
                violations.Add(Describe(key, in read, $"a value below {highest.Value}, seen by the read [{highest.Read.Start}, {highest.Read.End}] that returned before it started"));
        }
    }

    /// <summary>Checks the single-writer contract of a key: positive values that increase in start order, one call at a time.</summary>
    /// <param name="key">The register key.</param>
    /// <param name="writes">The writes of the key; sorted by start on return.</param>
    /// <param name="violations">Receives one line when the contract is broken.</param>
    /// <returns><see langword="true" /> when the writes of the key keep the contract, so its reads can be checked.</returns>
    private static bool CheckWriter(string key, List<RegisterWrite> writes, List<string> violations)
    {
        writes.Sort(static (a, b) => a.Start.CompareTo(b.Start));
        var sorted = CollectionsMarshal.AsSpan(writes);
        for (var i = 0; i < sorted.Length; i++)
        {
            ref readonly var write = ref sorted[i];
            var ordered = write.Value > 0 && write.End >= write.Start && (i == 0 || (write.Value > sorted[i - 1].Value && write.Start >= sorted[i - 1].End));
            if (!ordered)
            {
                violations.Add(
                    string.Create(
                        CultureInfo.InvariantCulture,
                        $"key {key}: write of {write.Value} at [{write.Start}, {write.End}] breaks the single writer contract: positive values, increasing, one call at a time"));
                return false;
            }
        }

        return true;
    }

    private static string Describe(string key, in RegisterRead read, string what) =>
        string.Create(CultureInfo.InvariantCulture, $"key {key}: read at [{read.Start}, {read.End}] saw {read.Observed}, {what}");

    private static Dictionary<string, List<RegisterRead>> GroupReads(IReadOnlyList<RegisterRead> reads)
    {
        var byKey = new Dictionary<string, List<RegisterRead>>(StringComparer.Ordinal);
        for (var i = 0; i < reads.Count; i++)
        {
            var read = reads[i];
            if (!byKey.TryGetValue(read.Key, out var list))
            {
                list = [];
                byKey.Add(read.Key, list);
            }

            list.Add(read);
        }

        return byKey;
    }

    private static Dictionary<string, List<RegisterWrite>> GroupWrites(IReadOnlyList<RegisterWrite> writes)
    {
        var byKey = new Dictionary<string, List<RegisterWrite>>(StringComparer.Ordinal);
        for (var i = 0; i < writes.Count; i++)
        {
            var write = writes[i];
            if (!byKey.TryGetValue(write.Key, out var list))
            {
                list = [];
                byKey.Add(write.Key, list);
            }

            list.Add(write);
        }

        return byKey;
    }
}
