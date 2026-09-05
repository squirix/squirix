using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Squirix.Server.Node.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Replication;

/// <summary>Bootstrap manifest store boundary enforcement and round-trips.</summary>
public sealed class BootstrapManifestStoreTests : ServerUnitTestBase
{
    /// <summary>Publication rejects group counts above the reader maximum before encoding.</summary>
    [Fact]
    public async Task PublishRejectsOverMaximumGroupCount()
    {
        using var dir = new TempDirectory("squirix-manifest-groups-over");
        var store = new BootstrapManifestStore(dir);
        var groups = new List<BootstrapGroupProgress>(100_001);
        for (var i = 0; i < 100_001; i++)
            groups.Add(new BootstrapGroupProgress($"group-{i}", BootstrapGroupState.Pending));

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(store.PublishAsync(Manifest(groups), DefaultCancellationToken));
    }

    /// <summary>Publication accepts exactly the maximum group count and reads it back.</summary>
    [Fact]
    public async Task PublishAcceptsMaximumGroupCount()
    {
        using var dir = new TempDirectory("squirix-manifest-groups-max");
        var store = new BootstrapManifestStore(dir);
        var groups = new List<BootstrapGroupProgress>(100_000);
        for (var i = 0; i < 100_000; i++)
            groups.Add(new BootstrapGroupProgress($"group-{i}", BootstrapGroupState.Pending));

        await store.PublishAsync(Manifest(groups), DefaultCancellationToken);
        var decoded = await store.ReadAsync(DefaultCancellationToken);

        Assert.NotNull(decoded);
        Assert.Equal(100_000, decoded.Groups.Count);
    }

    /// <summary>Publication rejects group identifiers above the reader string limit before encoding.</summary>
    [Fact]
    public async Task PublishRejectsOverMaximumStringLength()
    {
        using var dir = new TempDirectory("squirix-manifest-string-over");
        var store = new BootstrapManifestStore(dir);
        var manifest = Manifest([new BootstrapGroupProgress(new string('g', 4097), BootstrapGroupState.Pending)]);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(store.PublishAsync(manifest, DefaultCancellationToken));
    }

    /// <summary>Publication accepts exactly the maximum string length and reads it back.</summary>
    [Fact]
    public async Task PublishAcceptsMaximumStringLength()
    {
        using var dir = new TempDirectory("squirix-manifest-string-max");
        var store = new BootstrapManifestStore(dir);
        var groupId = new string('g', 4096);

        await store.PublishAsync(Manifest([new BootstrapGroupProgress(groupId, BootstrapGroupState.Pending)]), DefaultCancellationToken);
        var decoded = await store.ReadAsync(DefaultCancellationToken);

        Assert.NotNull(decoded);
        Assert.Equal(groupId, decoded.Groups[0].GroupId);
    }

    private static BootstrapManifest Manifest(IReadOnlyList<BootstrapGroupProgress> groups) => new()
    {
        Groups = groups,
        SourceClusterId = "cluster-a",
        SourceFingerprint = new byte[32],
        SourceGeneration = 1UL,
        TargetFingerprint = new byte[32],
        TargetGeneration = 2UL,
        TargetReplicaCount = 3,
    };
}
