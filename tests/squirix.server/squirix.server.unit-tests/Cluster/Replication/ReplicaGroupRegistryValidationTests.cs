using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Validation tests for <see cref="ReplicaGroupRegistry" /> construction and lifecycle.</summary>
[Immutable]
public sealed class ReplicaGroupRegistryValidationTests : ServerUnitTestBase
{
    /// <summary>Verifies that an out-of-range replica count is rejected.</summary>
    [Test]
    public void BadReplicaCountIsRejected() =>
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(0, static count => _ = new ReplicaGroupRegistry("root", ["node-a"], count, Fingerprint(), 1));

    /// <summary>Verifies that duplicated group identifiers are rejected.</summary>
    [Test]
    public void DuplicateGroupsAreRejected() =>
        _ = NodeExceptionAssert.For<ArgumentException>().Throws(static () => _ = new ReplicaGroupRegistry("root", ["node-a", "node-a"], 1, Fingerprint(), 1));

    /// <summary>Verifies that eligibility lookup before opening is rejected.</summary>
    [Test]
    public async Task EligibilityBeforeOpenIsRejectedAsync()
    {
        await using var registry = new ReplicaGroupRegistry("test-root", ["node-a"], 1, Fingerprint(), 1);

        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(registry, static r => _ = r.EligibilityFor("node-a"));
    }

    /// <summary>Verifies that an empty topology fingerprint is rejected.</summary>
    [Test]
    public void EmptyFingerprintIsRejected() => _ = NodeExceptionAssert.For<ArgumentException>()
                                                                       .Throws(static () => _ = new ReplicaGroupRegistry("root", ["node-a"], 1, ReadOnlyMemory<byte>.Empty, 1));

    /// <summary>Verifies that an empty persistence root is rejected.</summary>
    [Test]
    public void EmptyRootIsRejected()
    {
        _ = NodeExceptionAssert.For<ArgumentException>().Throws(
            string.Empty,
            Fingerprint(),
            static (root, fingerprint) => _ = new ReplicaGroupRegistry(root, ["node-a"], 1, fingerprint, 1));
    }

    /// <summary>Verifies that missing group identifiers are rejected.</summary>
    [Test]
    public void NullGroupsAreRejected()
    {
        IReadOnlyList<string>? groups = null;

        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(groups, static g => _ = new ReplicaGroupRegistry("root", g!, 1, Fingerprint(), 1));
    }

    /// <summary>Verifies that opening the registry twice is rejected.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OpenTwiceIsRejectedAsync(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-registry-open");
        await using var registry = new ReplicaGroupRegistry(dir, ["node-a"], 1, Fingerprint(), 1);
        await registry.OpenAsync(cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(registry.OpenAsync(cancellationToken));
    }

    /// <summary>Verifies that lookup of an unserved group misses.</summary>
    [Test]
    public async Task UnknownGroupLookupMissesAsync()
    {
        await using var registry = new ReplicaGroupRegistry("test-root", ["node-a"], 1, Fingerprint(), 1);

        _ = await Assert.That(registry.TryGetLog("unknown-group", out var log)).IsFalse();
        _ = await Assert.That(log).IsNull();
    }

    /// <summary>Creates a valid topology fingerprint for registry tests.</summary>
    /// <returns>A non-empty fingerprint.</returns>
    private static ReadOnlyMemory<byte> Fingerprint() => new([9]);
}
