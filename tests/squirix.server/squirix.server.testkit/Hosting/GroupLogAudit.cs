using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>
/// Audits the committed log of one replica group across its members once the group is quiet: no client operation is committed twice, and
/// every member retains the same committed entries.
/// </summary>
/// <remarks>
/// The group is quiet once every member applied up to its commit index and that commit index is the one of the leader. Only the retained
/// committed entries are compared: entries a member compacted into a snapshot are not seen.
/// </remarks>
internal static class GroupLogAudit
{
    private const int PageSize = 512;

    /// <summary>Waits until a group is quiet, then audits the committed log of each of its members.</summary>
    /// <typeparam name="TOptions">Node startup options type of the cluster.</typeparam>
    /// <param name="cluster">The cluster.</param>
    /// <param name="groupId">The replica group.</param>
    /// <param name="members">The members to audit; each must run, and one of them must lead the group.</param>
    /// <param name="bound">The longest wait for the group to become quiet.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>What the audit compared.</returns>
    /// <exception cref="ArgumentException">A member does not run.</exception>
    /// <exception cref="TimeoutException">The group did not become quiet within the bound.</exception>
    /// <exception cref="InvalidOperationException">A client operation was committed twice, or two members retain different committed entries.</exception>
    internal static async Task<GroupLogAuditReport> RunAsync<TOptions>(
        TestCluster<TOptions> cluster,
        string groupId,
        IReadOnlyList<string> members,
        TimeSpan bound,
        CancellationToken cancellationToken)
        where TOptions : ClusterStartOptions
    {
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentNullException.ThrowIfNull(members);
        var hosts = new ITestNodeHost[members.Count];
        for (var i = 0; i < hosts.Length; i++)
            hosts[i] = cluster.TryGetNode(members[i], out var host) ? host : throw new ArgumentException($"Member {members[i]} does not run.", nameof(members));

        try
        {
            await (Hosts: hosts, Group: groupId).WaitUntilValueAsync(static (s, token) => IsQuietAsync(s.Hosts, s.Group, token), bound, cancellationToken).ConfigureAwait(false);
        }
        catch (TimeoutException exception)
        {
            throw new TimeoutException($"Timed out waiting until group {groupId} is quiet on {string.Join(", ", members)}.", exception);
        }

        var logs = new Dictionary<string, IReadOnlyList<AuditedLogEntry>>(StringComparer.Ordinal);
        for (var i = 0; i < hosts.Length; i++)
            logs.Add(members[i], await ReadCommittedAsync(Log(hosts[i], groupId), cancellationToken).ConfigureAwait(false));

        var findings = new List<string>();
        foreach (var (member, entries) in logs)
            FindRepeatedOperations(member, entries, findings);

        var report = CompareCommittedRanges(logs, findings);
        return findings.Count == 0 ? report
            : throw new InvalidOperationException($"The committed log of group {groupId} failed its audit:{Environment.NewLine}{string.Join(Environment.NewLine, findings)}");
    }

    /// <summary>Compares the committed range every member retains, entry by entry: term, operation identifier and payload hash.</summary>
    /// <param name="logs">The retained committed entries of each member, dense and in index order.</param>
    /// <param name="findings">Receives one line per index at which two members differ.</param>
    /// <returns>The shared range and its number of client entries.</returns>
    internal static GroupLogAuditReport CompareCommittedRanges(IReadOnlyDictionary<string, IReadOnlyList<AuditedLogEntry>> logs, List<string> findings)
    {
        ArgumentNullException.ThrowIfNull(logs);
        ArgumentNullException.ThrowIfNull(findings);
        var from = 1UL;
        var to = ulong.MaxValue;
        string? reference = null;
        foreach (var (member, entries) in logs)
        {
            reference ??= member;
            if (entries.Count == 0)
            {
                to = 0UL;
                continue;
            }

            from = Math.Max(from, entries[0].Index);
            to = Math.Min(to, entries[^1].Index);
        }

        if (reference == null || to < from)
            return new GroupLogAuditReport(from, to == ulong.MaxValue ? 0UL : to, 0);

        var clientEntries = 0;
        var expected = logs[reference];
        for (var index = from; index <= to; index++)
        {
            var entry = At(expected, index);
            clientEntries += entry.IsClientEntry ? 1 : 0;
            foreach (var (member, entries) in logs)
            {
                var other = At(entries, index);
                if (other.Term != entry.Term || !string.Equals(other.OperationId, entry.OperationId, StringComparison.Ordinal) || other.PayloadHash != entry.PayloadHash)
                    findings.Add($"Index {index}: {reference} holds {Describe(in entry)} but {member} holds {Describe(in other)}.");
            }
        }

        return new GroupLogAuditReport(from, to, clientEntries);
    }

    /// <summary>Finds client operations a member committed more than once; leader no-ops and expiry tombstones are not client operations.</summary>
    /// <param name="member">The member the entries belong to.</param>
    /// <param name="entries">The committed entries of the member.</param>
    /// <param name="findings">Receives one line per repeated operation.</param>
    internal static void FindRepeatedOperations(string member, IReadOnlyList<AuditedLogEntry> entries, List<string> findings)
    {
        ArgumentNullException.ThrowIfNull(entries);
        ArgumentNullException.ThrowIfNull(findings);
        var seen = new Dictionary<(string Scope, string OperationId), ulong>();
        for (var i = 0; i < entries.Count; i++)
        {
            var entry = entries[i];
            if (!entry.IsClientEntry)
                continue;

            if (!seen.TryAdd((entry.OperationScope, entry.OperationId), entry.Index))
                findings.Add($"{member}: operation {entry.OperationScope}/{entry.OperationId} is committed at {seen[(entry.OperationScope, entry.OperationId)]} and again at {entry.Index}.");
        }
    }

    private static AuditedLogEntry At(IReadOnlyList<AuditedLogEntry> entries, ulong index) => entries[Convert.ToInt32(index - entries[0].Index)];

    private static string Describe(in AuditedLogEntry entry) =>
        $"term {entry.Term}, {entry.MutationKind} {entry.OperationScope}/{entry.OperationId}, payload {entry.PayloadHash:x16}";

    private static async ValueTask<bool> IsQuietAsync(ITestNodeHost[] hosts, string groupId, CancellationToken cancellationToken)
    {
        ulong? leaderCommit = null;
        for (var i = 0; i < hosts.Length && leaderCommit == null; i++)
        {
            if (hosts[i].GetRequiredService<IGroupLeaderTable>().HasLocalAuthority(groupId, out _))
                leaderCommit = (await Log(hosts[i], groupId).GetStatusAsync(cancellationToken).ConfigureAwait(false)).CommitIndex;
        }

        if (leaderCommit is not { } commit)
            return false;

        for (var i = 0; i < hosts.Length; i++)
        {
            var status = await Log(hosts[i], groupId).GetStatusAsync(cancellationToken).ConfigureAwait(false);
            if (status.CommitIndex != commit || hosts[i].GetRequiredService<ReplicaGroupAppliers>().For(groupId).AppliedIndex != commit)
                return false;
        }

        return true;
    }

    private static IFollowerLog Log(ITestNodeHost host, string groupId) =>
        host.GetRequiredService<ReplicaGroupRegistry>().TryGetLog(groupId, out var log) ? log : throw new InvalidOperationException($"The group log {groupId} is not open.");

    /// <summary>Reads the retained committed entries of a log, starting over when a compaction moves the retained range during the read.</summary>
    /// <param name="log">The group log.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The committed entries, dense and in index order.</returns>
    /// <exception cref="InvalidOperationException">The retained range moved on every attempt, or an entry does not decode.</exception>
    private static async Task<IReadOnlyList<AuditedLogEntry>> ReadCommittedAsync(IFollowerLog log, CancellationToken cancellationToken)
    {
        for (var attempt = 0; attempt < 3; attempt++)
        {
            var commit = (await log.GetStatusAsync(cancellationToken).ConfigureAwait(false)).CommitIndex;
            var next = (await log.GetRetentionAsync(cancellationToken).ConfigureAwait(false)).SnapshotIndex + 1UL;
            var entries = new List<AuditedLogEntry>();
            var retained = true;
            while (retained && next <= commit)
            {
                var read = await log.ReadEntriesAsync(next, PageSize, cancellationToken).ConfigureAwait(false);
                retained = read.Retained && read.Entries.Count > 0;
                for (var i = 0; i < read.Entries.Count && next <= commit; i++, next++)
                {
                    var entry = read.Entries[i];
                    entries.Add(ToAudited(in entry));
                }
            }

            if (retained)
                return entries;
        }

        throw new InvalidOperationException($"The retained range of group log {log.GroupId} moved during every read.");
    }

    private static AuditedLogEntry ToAudited(in FollowerLogEntry entry)
    {
        if (ReplicaLogCodec.Decode(entry.Payload) is not { } record)
            throw new InvalidOperationException($"The entry at {entry.LogIndex} of a group log does not decode.");

        Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
        _ = SHA256.HashData(entry.PayloadSpan, hash);
        return new AuditedLogEntry(entry.LogIndex, entry.Term, record.OperationScope, record.OperationId, record.MutationKind, BinaryPrimitives.ReadUInt64BigEndian(hash));
    }
}
