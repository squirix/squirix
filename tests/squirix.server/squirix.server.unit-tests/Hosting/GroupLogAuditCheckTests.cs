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

        _ = GroupLogAudit.CompareCommittedRanges(logs, findings);

        _ = await Assert.That(findings.Count).IsEqualTo(1);
        _ = await Assert.That(findings[0]).StartsWith("Index 2:", StringComparison.Ordinal);
    }

    /// <summary>Only the range every member retains is compared: a compacted prefix of one member narrows it.</summary>
    [Test]
    public async Task SharedRangeSkipsCompactedPrefix()
    {
        List<string> findings = [];
        var logs = new Dictionary<string, IReadOnlyList<AuditedLogEntry>>(StringComparer.Ordinal)
        {
            ["node-a"] = [Client(1, 1, "op-1"), Client(2, 1, "op-2"), Entry(3, 2, string.Empty, ReplicaMutationKinds.LeaderNoop)],
            ["node-b"] = [Client(2, 1, "op-2"), Entry(3, 2, string.Empty, ReplicaMutationKinds.LeaderNoop)],
        };

        var report = GroupLogAudit.CompareCommittedRanges(logs, findings);

        _ = await Assert.That(findings.Count).IsEqualTo(0);
        _ = await Assert.That(report).IsEqualTo(new GroupLogAuditReport(2, 3, 1));
    }

    private static AuditedLogEntry Client(ulong index, ulong term, string operationId) => Entry(index, term, operationId, ReplicaMutationKinds.Set);

    private static AuditedLogEntry Entry(ulong index, ulong term, string operationId, string kind) =>
        new(index, term, Scope, operationId, kind, (index * 31UL) + term);
}
