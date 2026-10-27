using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Node ids name replica group directories, so their length is bounded by the file-name limit.</summary>
[Immutable]
public sealed class GroupIdLengthTests
{
    private const int FileNameLimit = 255;
    private const int TwoByteCharCount = 63;
    private const string TwoByteChar = "é";

    /// <summary>A node id of the maximum length yields a directory name within the file-name limit.</summary>
    [Test]
    public async Task EncodedSegmentAtLimitFitsFileName()
    {
        var segment = GroupStoragePaths.EncodeGroupSegment(new string('a', GroupStoragePaths.MaxGroupIdUtf8Bytes));

        _ = await Assert.That(segment.Length).IsLessThanOrEqualTo(FileNameLimit);
        _ = await Assert.That(GroupStoragePaths.EncodeGroupSegment(new string('a', GroupStoragePaths.MaxGroupIdUtf8Bytes + 1)).Length).IsGreaterThan(FileNameLimit);
    }

    /// <summary>A node id at the byte limit is accepted with replication.</summary>
    [Test]
    public async Task AcceptsNodeIdAtLimit()
    {
        var topology = Topology(new string('a', GroupStoragePaths.MaxGroupIdUtf8Bytes), 2);

        _ = await Assert.That(Validate(topology, out _)).IsTrue();
    }

    /// <summary>A node id over the byte limit is rejected with replication.</summary>
    [Test]
    public async Task RejectsNodeIdOverLimit()
    {
        var topology = Topology(new string('a', GroupStoragePaths.MaxGroupIdUtf8Bytes + 1), 2);

        _ = await Assert.That(Validate(topology, out var errors)).IsFalse();
        _ = await Assert.That(errors).Contains(static error => error.Contains("UTF-8 bytes", StringComparison.Ordinal));
        _ = await Assert.That(GroupStoragePaths.MaxGroupIdUtf8Bytes).IsEqualTo(125);
    }

    /// <summary>The limit counts UTF-8 bytes, not characters.</summary>
    [Test]
    public async Task CountsUtf8BytesNotCharacters()
    {
        var withinLimit = Topology(new string(TwoByteChar[0], TwoByteCharCount - 1), 2);
        var overLimit = Topology(new string(TwoByteChar[0], TwoByteCharCount), 2);

        _ = await Assert.That(Validate(withinLimit, out _)).IsTrue();
        _ = await Assert.That(Validate(overLimit, out _)).IsFalse();
    }

    /// <summary>A peer node id over the byte limit is rejected with replication.</summary>
    [Test]
    public async Task RejectsPeerNodeIdOverLimit()
    {
        var topology = Topology("n1", 2, new string('b', GroupStoragePaths.MaxGroupIdUtf8Bytes + 1));

        _ = await Assert.That(Validate(topology, out var errors)).IsFalse();
        _ = await Assert.That(errors).Contains(static error => error.StartsWith("Peers[].NodeId", StringComparison.Ordinal) && error.Contains("UTF-8 bytes", StringComparison.Ordinal));
    }

    /// <summary>Without replication no group directory exists, so the byte limit does not apply.</summary>
    [Test]
    public async Task IgnoresByteLimitWithoutReplication()
    {
        var topology = Topology(new string('a', GroupStoragePaths.MaxGroupIdUtf8Bytes + 1), 1);

        _ = await Assert.That(Validate(topology, out _)).IsTrue();
    }

    private static bool Validate(TopologyOptions topology, out List<string> errors)
    {
        errors = [];
        ReplicaGroupIdLimit.CollectFailures(errors, topology);
        return errors.Count == 0;
    }

    private static TopologyOptions Topology(string nodeId, int replicaCount, string? otherNodeId = null)
    {
        var peers = new[]
        {
            new ServerPeer { NodeId = nodeId, Uri = PeerUri(1) },
            new ServerPeer { NodeId = otherNodeId ?? "n2", Uri = PeerUri(2) },
        };

        return new TopologyOptions(peers)
        {
            ClusterId = "c1",
            NodeId = nodeId,
            Uri = peers[0].Uri,
            ReplicaCount = replicaCount,
        };
    }

    private static Uri PeerUri(int index) => new("https://localhost:" + (6000 + index).ToString(CultureInfo.InvariantCulture));
}
