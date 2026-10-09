using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Threading;

namespace Squirix.E2ETests.Fixtures;

/// <summary>
/// Records the calls on a set of single-writer registers and checks necessary conditions of linearizability on every read: one cache key
/// per register, written by one writer with increasing positive integers.
/// </summary>
/// <remarks>
/// <para>
/// The model is textbook linearizability. An acknowledged write takes effect between its start and its end. A failed write is
/// ambiguous: it never completed, so it may take effect at any point after its start, after later acknowledged writes of the same writer
/// included (a forwarded request held by a partition can be appended late), or never. Zero stands for not found, the register before
/// its first write. Real time orders two calls only when one ended strictly before the other started; equal timestamps overlap.
/// </para>
/// <para>Every successful read of a key must satisfy three rules, each a necessary condition in that model:</para>
/// <list type="bullet">
///   <item>it sees zero or a value some write of the key wrote, and that write started no later than the read returned;</item>
///   <item>
///     when it sees zero or an acknowledged value, the value is not below an acknowledged write that ended before the read started,
///     so no acknowledged write is lost; a lower ambiguous value is allowed, since its write may have taken effect late;
///   </item>
///   <item>
///     when it sees zero or an acknowledged value, the value is not below one a read that ended before it started saw, so reads never
///     go back to an acknowledged past; again a lower ambiguous value is allowed.
///   </item>
/// </list>
/// <para>
/// The check is sound: it never fails a linearizable history. It is not complete, since it tests each rule pair by pair and does not
/// search for one total order. It misses, for example, a read that sees a newer acknowledged value again after an earlier read saw the
/// late effect of an older ambiguous write, which it allows on its own. Failed reads impose nothing and are only counted. Recording is thread
/// safe; <see cref="Check(IReadOnlyList{RegisterWrite}, IReadOnlyList{RegisterRead})" /> is a pure function of the records.
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

    /// <summary>Checks a history and returns every violation found, in key order; an empty list means no rule of the check failed.</summary>
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
                CheckMonotonic(key, keyWrites, keyReads, violations);
            }
        }

        return violations;
    }

    /// <summary>Checks the recorded history.</summary>
    /// <returns>One line per violation; empty when no rule of the check failed.</returns>
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

    /// <summary>Counts the acknowledged writes and the successful reads that started at or after a point in time.</summary>
    /// <param name="timestamp">The point in time, in <see cref="System.Diagnostics.Stopwatch" /> ticks.</param>
    /// <returns>The number of acknowledged writes and of successful reads that started at or after it.</returns>
    internal (int AckedWrites, int Reads) StartedAfter(long timestamp)
    {
        var (writes, reads) = (0, 0);
        lock (_gate)
        {
            foreach (ref readonly var write in CollectionsMarshal.AsSpan(_writes))
                writes += write.Acked && write.Start >= timestamp ? 1 : 0;

            foreach (ref readonly var read in CollectionsMarshal.AsSpan(_reads))
                reads += read.Start >= timestamp ? 1 : 0;
        }

        return (writes, reads);
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

    /// <summary>
    /// Checks each read of a key against the writes: a value some write wrote, a write that started before the read returned, and no
    /// acknowledged value, or not found, once a later write was acknowledged before the read started.
    /// </summary>
    /// <param name="key">The register key.</param>
    /// <param name="writes">The writes of the key.</param>
    /// <param name="reads">The reads of the key.</param>
    /// <param name="violations">Receives one line per violation.</param>
    private static void CheckBounds(string key, List<RegisterWrite> writes, List<RegisterRead> reads, List<string> violations)
    {
        var keyWrites = CollectionsMarshal.AsSpan(writes);
        foreach (ref readonly var read in CollectionsMarshal.AsSpan(reads))
        {
            var (seen, ackedBefore) = Scan(keyWrites, in read);

            // Only not found, or an acknowledged value, is pinned before a later acknowledged write: an ambiguous write may take effect
            // at any time after it started, after later writes included.
            var pinned = read.Observed == 0 || seen.Acked;
            if (!seen.Found)
                violations.Add(Describe(key, in read, "a value no write wrote"));
            else if (seen.Start > read.End)
                violations.Add(Describe(key, in read, "a value whose write started after the read returned"));
            else if (pinned && read.Observed < ackedBefore)
                violations.Add(Describe(key, in read, $"a value below {ackedBefore}, acknowledged before the read started, so an acknowledged write is lost"));
        }
    }

    /// <summary>
    /// Checks that no read of a key sees not found, or an acknowledged value, below a value a read that returned before it started saw;
    /// a lower ambiguous value may still take effect late.
    /// </summary>
    /// <param name="key">The register key.</param>
    /// <param name="writes">The writes of the key.</param>
    /// <param name="reads">The reads of the key.</param>
    /// <param name="violations">Receives one line per violation.</param>
    private static void CheckMonotonic(string key, List<RegisterWrite> writes, List<RegisterRead> reads, List<string> violations)
    {
        var acked = new HashSet<long>();
        foreach (ref readonly var write in CollectionsMarshal.AsSpan(writes))
        {
            if (write.Acked)
                _ = acked.Add(write.Value);
        }

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

            var pinned = read.Observed == 0 || acked.Contains(read.Observed);
            if (pinned && read.Observed < highest.Value)
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

    /// <summary>Finds the write a read saw, and the highest write acknowledged before the read started.</summary>
    /// <param name="writes">The writes of the key.</param>
    /// <param name="read">The read.</param>
    /// <returns>
    /// Whether the seen value was written (zero always is), when its write started and whether it was acknowledged, and the highest value
    /// acknowledged before the read started, zero when none was.
    /// </returns>
    private static ((bool Found, long Start, bool Acked) Seen, long AckedBefore) Scan(ReadOnlySpan<RegisterWrite> writes, in RegisterRead read)
    {
        var seen = (Found: read.Observed == 0, Start: long.MinValue, Acked: false);
        var ackedBefore = 0L;
        foreach (ref readonly var write in writes)
        {
            if (write.Value == read.Observed)
                seen = (true, write.Start, write.Acked);

            if (write.Acked && write.End < read.Start)
                ackedBefore = Math.Max(ackedBefore, write.Value);
        }

        return (seen, ackedBefore);
    }
}
