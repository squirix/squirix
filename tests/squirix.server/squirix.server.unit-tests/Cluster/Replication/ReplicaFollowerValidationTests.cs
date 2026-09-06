using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Validation tests for <see cref="ReplicaFollower" /> group resolution.</summary>
[Immutable]
public sealed class ReplicaFollowerValidationTests : ServerUnitTestBase
{
    /// <summary>Verifies that the follower requires a group registry.</summary>
    [Fact]
    public void FollowerRequiresRegistry()
    {
        ReplicaGroupRegistry? registry = null;

        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(registry, static r => _ = new ReplicaFollower(r!));
    }

    /// <summary>Verifies that commit advance on an unserved group is refused.</summary>
    [Fact]
    public async Task AdvanceOnUnknownGroupIsRefusedAsync()
    {
        await using var registry = CreateClosedRegistry();
        var follower = new ReplicaFollower(registry);

        var result = await follower.AdvanceCommitAsync("unknown-group", ReadOnlyMemory<byte>.Empty, 1, 0, 7, DefaultCancellationToken);

        Assert.False(result.Success);
        Assert.Equal(FollowerLogRefusal.NotMember, result.RefusalCode);
    }

    /// <summary>Verifies that appends on an unserved group are refused.</summary>
    [Fact]
    public async Task AppendOnUnknownGroupIsRefusedAsync()
    {
        await using var registry = CreateClosedRegistry();
        var follower = new ReplicaFollower(registry);
        var batch = new FollowerBatch([], "node-a", 7, 0, 0, 0);

        var result = await follower.AppendAsync("unknown-group", ReadOnlyMemory<byte>.Empty, 1, batch, DefaultCancellationToken);

        Assert.False(result.Success);
        Assert.Equal(FollowerLogRefusal.NotMember, result.RefusalCode);
    }

    /// <summary>Verifies that status on an unserved group is missing.</summary>
    [Fact]
    public async Task StatusOnUnknownGroupIsNullAsync()
    {
        await using var registry = CreateClosedRegistry();
        var follower = new ReplicaFollower(registry);

        var status = await follower.GetStatusAsync("unknown-group", DefaultCancellationToken);

        Assert.Null(status);
    }

    /// <summary>Verifies that snapshot install on an unserved group is refused.</summary>
    [Fact]
    public async Task InstallOnUnknownGroupIsRefusedAsync()
    {
        await using var registry = CreateClosedRegistry();
        var follower = new ReplicaFollower(registry);
        var snapshot = new GroupSnapshot("unknown-group", ReadOnlyMemory<byte>.Empty, 1, 0, 0, 0, []);

        var result = await follower.InstallSnapshotAsync("unknown-group", ReadOnlyMemory<byte>.Empty, 1, snapshot, 7, DefaultCancellationToken);

        Assert.False(result.Success);
        Assert.Equal(FollowerLogRefusal.NotMember, result.Refusal);
    }

    /// <summary>Verifies that snapshot upload on an unserved group is refused.</summary>
    [Fact]
    public async Task InstallUploadOnUnknownGroupRefusedAsync()
    {
        await using var registry = CreateClosedRegistry();
        var follower = new ReplicaFollower(registry);
        var upload = new ReplicaSnapshotUpload(ReadOnlyMemory<byte>.Empty, 0, 0, 0);

        var result = await follower.InstallSnapshotUploadAsync("unknown-group", ReadOnlyMemory<byte>.Empty, 1, upload, 7, DefaultCancellationToken);

        Assert.False(result.Success);
        Assert.Equal(FollowerLogRefusal.NotMember, result.Refusal);
    }

    /// <summary>Creates a registry that was never opened, so every lookup misses.</summary>
    /// <returns>The closed registry.</returns>
    private static ReplicaGroupRegistry CreateClosedRegistry() =>
        new("test-root", ["node-a"], 1, new ReadOnlyMemory<byte>([9]), 1);
}
