using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Validation tests for <see cref="ReplicaFollower" /> group resolution.</summary>
[Immutable]
public sealed class ReplicaFollowerValidationTests : ServerUnitTestBase
{
    /// <summary>Verifies that commit advance on an unserved group is refused.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AdvanceOnUnknownGroupIsRefusedAsync(CancellationToken cancellationToken)
    {
        await using var registry = CreateClosedRegistry();
        var follower = new ReplicaFollower(registry);

        var result = await follower.AdvanceCommitAsync("unknown-group", ReadOnlyMemory<byte>.Empty, 1, 0, 7, cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.NotMember);
    }

    /// <summary>Verifies that appends on an unserved group are refused.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppendOnUnknownGroupIsRefusedAsync(CancellationToken cancellationToken)
    {
        await using var registry = CreateClosedRegistry();
        var follower = new ReplicaFollower(registry);
        var batch = new FollowerBatch([], "node-a", 7, 0, 0, 0);

        var result = await follower.AppendAsync("unknown-group", ReadOnlyMemory<byte>.Empty, 1, batch, cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.NotMember);
    }

    /// <summary>Verifies that the follower requires a group registry.</summary>
    [Test]
    public void FollowerRequiresRegistry()
    {
        ReplicaGroupRegistry? registry = null;

        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(registry, static r => _ = new ReplicaFollower(r!));
    }

    /// <summary>Verifies that snapshot install on an unserved group is refused.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallOnUnknownGroupIsRefusedAsync(CancellationToken cancellationToken)
    {
        await using var registry = CreateClosedRegistry();
        var follower = new ReplicaFollower(registry);
        var snapshot = new GroupSnapshot("unknown-group", ReadOnlyMemory<byte>.Empty, 1, 0, 0, 0, []);

        var result = await follower.InstallSnapshotAsync("unknown-group", ReadOnlyMemory<byte>.Empty, 1, snapshot, 7, cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.Refusal).IsEqualTo(FollowerLogRefusal.NotMember);
    }

    /// <summary>Verifies that snapshot upload on an unserved group is refused.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InstallUploadOnUnknownGroupRefusedAsync(CancellationToken cancellationToken)
    {
        await using var registry = CreateClosedRegistry();
        var follower = new ReplicaFollower(registry);
        var upload = new ReplicaSnapshotUpload(ReadOnlyMemory<byte>.Empty, 0, 0, 0);

        var result = await follower.InstallSnapshotUploadAsync("unknown-group", ReadOnlyMemory<byte>.Empty, 1, upload, 7, cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.Refusal).IsEqualTo(FollowerLogRefusal.NotMember);
    }

    /// <summary>Verifies that status on an unserved group is missing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StatusOnUnknownGroupIsNullAsync(CancellationToken cancellationToken)
    {
        await using var registry = CreateClosedRegistry();
        var follower = new ReplicaFollower(registry);

        var status = await follower.GetStatusAsync("unknown-group", cancellationToken);

        _ = await Assert.That(status).IsNull();
    }

    /// <summary>Creates a registry that was never opened, so every lookup misses.</summary>
    /// <returns>The closed registry.</returns>
    private static ReplicaGroupRegistry CreateClosedRegistry() => new("test-root", ["node-a"], 1, new ReadOnlyMemory<byte>([9]), 1);
}
