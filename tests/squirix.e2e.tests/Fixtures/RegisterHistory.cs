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
    private static readonly IComparer<RegisterWrite> ValueOrder = Comparer<RegisterWrite>.Create(static (left, right) => left.Value.CompareTo(right.Value));

    private readonly Lock _gate = new();
    private readonly Dictionary<string, long> _latestAcknowledgedStarts = [with(StringComparer.Ordinal)];
    private readonly List<RegisterRead> _reads = [];
    private readonly List<RegisterRead> _failedReadCalls = [];
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
            if (!CheckWriter(key, keyWrites, violations))
                continue;
            CheckBounds(key, keyWrites, keyReads, violations);
            CheckMonotonic(key, keyWrites, keyReads, violations);
        }

        return violations;
    }

    /// <summary>Checks the recorded history with the complete per-key linearizability search.</summary>
    /// <param name="cancellationToken">Cancels the search without returning a verdict.</param>
    /// <returns>One line per violation; empty when a legal order exists.</returns>
    internal List<string> Check(CancellationToken cancellationToken = default)
    {
        RegisterWrite[] writes;
        RegisterRead[] reads;
        lock (_gate)
        {
            writes = [.. _writes];
            reads = [.. _reads];
        }

        return RegisterLinearizability.Check(writes, reads, cancellationToken);
    }

    /// <summary>Captures an atomic snapshot for a portable history artifact.</summary>
    /// <returns>The writes, successful reads, failed read count and failed call intervals.</returns>
    internal (RegisterWrite[] Writes, RegisterRead[] Reads, int FailedReads, RegisterRead[] FailedCalls) Snapshot()
    {
        lock (_gate)
            return ([.. _writes], [.. _reads], _failedReads, [.. _failedReadCalls]);
    }

    /// <summary>Records a failed read interval for diagnostics; it does not constrain the register state.</summary>
    /// <param name="key">The register key.</param>
    /// <param name="start">Invocation timestamp.</param>
    /// <param name="end">Failure timestamp.</param>
    internal void RecordReadFailure(string key, long start, long end)
    {
        lock (_gate)
        {
            _failedReads++;
            _failedReadCalls.Add(new RegisterRead(key, start, end, 0));
        }
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

    /// <summary>Checks whether this key has an acknowledged write invoked at or after the recovery boundary.</summary>
    /// <param name="key">The recovered key.</param>
    /// <param name="timestamp">The inclusive invocation boundary.</param>
    /// <returns>Whether the key accepted a new write after recovery.</returns>
    internal bool HasAcknowledgedWriteSince(string key, long timestamp)
    {
        lock (_gate)
            return _latestAcknowledgedStarts.TryGetValue(key, out var latest) && latest >= timestamp;
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
            else if (!_latestAcknowledgedStarts.TryGetValue(write.Key, out var latest) || write.Start > latest)
                _latestAcknowledgedStarts[write.Key] = write.Start;
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
        if (reads.Count == 0)
            return;

        // CheckWriter established increasing values and nondecreasing completion times.
        var keyWrites = CollectionsMarshal.AsSpan(writes);
        var acknowledged = new long[keyWrites.Length];
        var highestAcknowledged = 0L;
        for (var i = 0; i < keyWrites.Length; i++)
        {
            if (keyWrites[i].Acked)
                highestAcknowledged = keyWrites[i].Value;

            acknowledged[i] = highestAcknowledged;
        }

        foreach (ref readonly var read in CollectionsMarshal.AsSpan(reads))
        {
            var (found, started, acked) = FindSeenWrite(keyWrites, read.Observed);
            var ackedBefore = HighestAcknowledgedBefore(keyWrites, acknowledged, read.Start);

            // Only not found, or an acknowledged value, is pinned before a later acknowledged write: an ambiguous write may take effect
            // at any time after it started, after later writes included.
            var pinned = read.Observed == 0 || acked;
            if (!found)
                violations.Add(Describe(key, in read, "a value no write wrote"));
            else if (started > read.End)
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

        RegisterRead[] byEnd = [.. reads];
        reads.Sort(static (a, b) => a.Start.CompareTo(b.Start));
        Array.Sort(byEnd, static (a, b) => a.End.CompareTo(b.End));

        // Sweep the reads in start order; the reads that returned before the current one started form a growing prefix of the end order.
        var returned = 0;
        var highest = (Value: 0L, Read: default(RegisterRead));
        foreach (ref readonly var read in CollectionsMarshal.AsSpan(reads))
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
            if (ordered)
                continue;
            var item = string.Create(
                CultureInfo.InvariantCulture,
                $"key {key}: write of {write.Value} at [{write.Start}, {write.End}] breaks the single writer contract: positive values, increasing, one call at a time");
            violations.Add(item);
            return false;
        }

        return true;
    }

    private static string Describe(string key, in RegisterRead read, string what) =>
        string.Create(CultureInfo.InvariantCulture, $"key {key}: read at [{read.Start}, {read.End}] saw {read.Observed}, {what}");

    /// <summary>Finds the write of the observed value without scanning the other writes.</summary>
    /// <param name="writes">Validated writes, sorted by increasing value.</param>
    /// <param name="observed">The observed value; zero denotes the initial register state.</param>
    /// <returns>Whether the value was written, when its write started, and whether it was acknowledged.</returns>
    private static (bool Found, long Start, bool Acked) FindSeenWrite(ReadOnlySpan<RegisterWrite> writes, long observed)
    {
        if (observed == 0)
            return (true, long.MinValue, false);

        var index = writes.BinarySearch(new RegisterWrite { Value = observed }, ValueOrder);
        return index >= 0
            ? (true, writes[index].Start, writes[index].Acked)
            : default;
    }

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

    /// <summary>Finds the highest acknowledged value among writes that ended strictly before a read started.</summary>
    /// <param name="writes">Validated writes, sorted by start and therefore by completion time.</param>
    /// <param name="acknowledged">The highest acknowledged value through each write position.</param>
    /// <param name="start">The read invocation timestamp.</param>
    /// <returns>The highest acknowledged value, or zero when no write was acknowledged before the read.</returns>
    private static long HighestAcknowledgedBefore(ReadOnlySpan<RegisterWrite> writes, ReadOnlySpan<long> acknowledged, long start)
    {
        var low = 0;
        var high = writes.Length;
        while (low < high)
        {
            var middle = low + ((high - low) / 2);
            if (writes[middle].End < start)
                low = middle + 1;
            else
                high = middle;
        }

        return low == 0 ? 0L : acknowledged[low - 1];
    }
}
