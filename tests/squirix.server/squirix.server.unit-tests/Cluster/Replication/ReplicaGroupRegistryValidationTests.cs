using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Validation tests for <see cref="ReplicaGroupRegistry" /> construction and lifecycle.</summary>
[Immutable]
public sealed class ReplicaGroupRegistryValidationTests : ServerUnitTestBase
{
    /// <summary>Verifies that an empty persistence root is rejected.</summary>
    [Fact]
    public void EmptyRootIsRejected()
    {
        _ = NodeExceptionAssert.For<ArgumentException>().Throws(
            string.Empty,
            Fingerprint(),
            static (root, fingerprint) => _ = new ReplicaGroupRegistry(root, ["node-a"], 1, fingerprint, 1));
    }

    /// <summary>Verifies that missing group identifiers are rejected.</summary>
    [Fact]
    public void NullGroupsAreRejected()
    {
        IReadOnlyList<string>? groups = null;

        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(groups, static g => _ = new ReplicaGroupRegistry("root", g!, 1, Fingerprint(), 1));
    }

    /// <summary>Verifies that an out-of-range replica count is rejected.</summary>
    [Fact]
    public void BadReplicaCountIsRejected()
    {
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(
            0,
            static count => _ = new ReplicaGroupRegistry("root", ["node-a"], count, Fingerprint(), 1));
    }

    /// <summary>Verifies that duplicated group identifiers are rejected.</summary>
    [Fact]
    public void DuplicateGroupsAreRejected() =>
        _ = NodeExceptionAssert.For<ArgumentException>().Throws(static () => _ = new ReplicaGroupRegistry("root", ["node-a", "node-a"], 1, Fingerprint(), 1));

    /// <summary>Verifies that an empty topology fingerprint is rejected.</summary>
    [Fact]
    public void EmptyFingerprintIsRejected() =>
        _ = NodeExceptionAssert.For<ArgumentException>().Throws(static () => _ = new ReplicaGroupRegistry("root", ["node-a"], 1, ReadOnlyMemory<byte>.Empty, 1));

    /// <summary>Verifies that opening the registry twice is rejected.</summary>
    [Fact]
    public async Task OpenTwiceIsRejectedAsync()
    {
        using var dir = new TempDirectory("squirix-registry-open");
        await using var registry = new ReplicaGroupRegistry(dir, ["node-a"], 1, Fingerprint(), 1);
        await registry.OpenAsync(DefaultCancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(registry.OpenAsync(DefaultCancellationToken));
    }

    /// <summary>Verifies that eligibility lookup before opening is rejected.</summary>
    [Fact]
    public async Task EligibilityBeforeOpenIsRejectedAsync()
    {
        await using var registry = new ReplicaGroupRegistry("test-root", ["node-a"], 1, Fingerprint(), 1);

        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(registry, static r => _ = r.EligibilityFor("node-a"));
    }

    /// <summary>Verifies that lookup of an unserved group misses.</summary>
    [Fact]
    public async Task UnknownGroupLookupMissesAsync()
    {
        await using var registry = new ReplicaGroupRegistry("test-root", ["node-a"], 1, Fingerprint(), 1);

        Assert.False(registry.TryGetLog("unknown-group", out var log));
        Assert.Null(log);
    }

    /// <summary>Creates a valid topology fingerprint for registry tests.</summary>
    /// <returns>A non-empty fingerprint.</returns>
    private static ReadOnlyMemory<byte> Fingerprint() => new([9]);
}
