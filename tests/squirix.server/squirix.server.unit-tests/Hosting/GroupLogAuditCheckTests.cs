using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>The checks of the group log audit find repeated client operations and diverging committed entries, and nothing else.</summary>
public sealed class GroupLogAuditCheckTests
{
    private const string Scope = "audit";

    /// <summary>A client operation committed twice is reported with both indexes.</summary>
    [Test]
    public async Task RepeatedOperationIsReported()
    {
        List<string> findings = [];

        GroupLogAudit.FindRepeatedOperations("node-a", [Client(1, 1, "op-1"), Client(2, 1, "op-2"), Client(3, 2, "op-1")], findings);

        _ = await Assert.That(findings.Count).IsEqualTo(1);
        _ = await Assert.That(findings[0]).Contains("operation audit/op-1 is committed at 1 and again at 3", StringComparison.Ordinal);
    }

    /// <summary>Leader no-ops and expiry tombstones may share an operation identifier: they are not client operations.</summary>
    [Test]
    public async Task NoopsAndTombstonesAreSkipped()
    {
        List<string> findings = [];
        AuditedLogEntry[] entries =
        [
            Entry(1, 1, string.Empty, ReplicaMutationKinds.LeaderNoop), Entry(2, 2, string.Empty, ReplicaMutationKinds.LeaderNoop),
            Entry(3, 2, "expire-1", ReplicaMutationKinds.Expire), Entry(4, 2, "expire-1", ReplicaMutationKinds.Expire),
        ];

        GroupLogAudit.FindRepeatedOperations("node-a", entries, findings);

        _ = await Assert.That(findings.Count).IsEqualTo(0);
    }

    /// <summary>Two members that hold different entries at one index of the shared range are reported at that index.</summary>
    [Test]
    public async Task DivergentEntryIsReported()
    {
        List<string> findings = [];
        var logs = new Dictionary<string, IReadOnlyList<AuditedLogEntry>>(StringComparer.Ordinal)
        {
            ["node-a"] = [Client(1, 1, "op-1"), Client(2, 1, "op-2")],
            ["node-b"] = [Client(1, 1, "op-1"), Client(2, 2, "op-2")],
        };

        _ = GroupLogAudit.CompareCommittedRanges(logs, 2, false, findings);

        _ = await Assert.That(findings.Count).IsEqualTo(1);
        _ = await Assert.That(findings[0]).StartsWith("Index 2:", StringComparison.Ordinal);
    }

    /// <summary>With compaction allowed, only the range every member retains is compared: a compacted prefix of one member narrows it.</summary>
    [Test]
    public async Task SharedRangeSkipsCompactedPrefix()
    {
        List<string> findings = [];

        var report = GroupLogAudit.CompareCommittedRanges(CompactedPrefix(), 3, true, findings);

        _ = await Assert.That(findings.Count).IsEqualTo(0);
        _ = await Assert.That(report).IsEqualTo(new GroupLogAuditReport(2, 3, 1));
    }

    /// <summary>By default a member that retains the committed log only from above index one fails the audit.</summary>
    [Test]
    public async Task CompactedPrefixIsReportedByDefault()
    {
        List<string> findings = [];

        _ = GroupLogAudit.CompareCommittedRanges(CompactedPrefix(), 3, false, findings);

        _ = await Assert.That(findings.Count).IsEqualTo(1);
        _ = await Assert.That(findings[0]).StartsWith("node-b retains the committed log only from 2", StringComparison.Ordinal);
    }

    /// <summary>A member that retains no committed entry leaves the shared range empty, which fails the audit unless compaction is allowed.</summary>
    [Test]
    public async Task EmptyMemberFailsUnlessAllowed()
    {
        List<string> strict = [];
        List<string> lenient = [];
        var logs = new Dictionary<string, IReadOnlyList<AuditedLogEntry>>(StringComparer.Ordinal)
        {
            ["node-a"] = [Client(1, 1, "op-1"), Client(2, 1, "op-2")],
            ["node-b"] = [],
        };

        var report = GroupLogAudit.CompareCommittedRanges(logs, 2, false, strict);
        _ = GroupLogAudit.CompareCommittedRanges(logs, 2, true, lenient);

        _ = await Assert.That(strict.Count).IsEqualTo(2);
        _ = await Assert.That(strict[0]).StartsWith("node-b retains no committed entry", StringComparison.Ordinal);
        _ = await Assert.That(strict[1]).StartsWith("No committed entry is retained by every member", StringComparison.Ordinal);
        _ = await Assert.That(lenient.Count).IsEqualTo(0);
        _ = await Assert.That(report).IsEqualTo(new GroupLogAuditReport(1, 0, 0));
    }

    private static Dictionary<string, IReadOnlyList<AuditedLogEntry>> CompactedPrefix() => new(StringComparer.Ordinal)
    {
        ["node-a"] = [Client(1, 1, "op-1"), Client(2, 1, "op-2"), Entry(3, 2, string.Empty, ReplicaMutationKinds.LeaderNoop)],
        ["node-b"] = [Client(2, 1, "op-2"), Entry(3, 2, string.Empty, ReplicaMutationKinds.LeaderNoop)],
    };

    private static AuditedLogEntry Client(ulong index, ulong term, string operationId) => Entry(index, term, operationId, ReplicaMutationKinds.Set);

    private static AuditedLogEntry Entry(ulong index, ulong term, string operationId, string kind) =>
        new(index, term, Scope, operationId, kind, (index * 31UL) + term);
}
