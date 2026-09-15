using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Node.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Replication;

/// <summary>Bootstrap manifest store boundary enforcement and round-trips.</summary>
public sealed class BootstrapManifestStoreTests : ServerUnitTestBase
{
    /// <summary>Publication accepts exactly the maximum group count and reads it back.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishAcceptsMaximumGroupCount(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-manifest-groups-max");
        var store = new BootstrapManifestStore(dir);
        var groups = new List<BootstrapGroupProgress>(100_000);
        for (var i = 0; i < 100_000; i++)
            groups.Add(new BootstrapGroupProgress($"group-{i}", BootstrapGroupState.Pending));

        await store.PublishAsync(Manifest(groups), cancellationToken);
        var decoded = await store.ReadAsync(cancellationToken);

        _ = await Assert.That(decoded).IsNotNull();
        _ = await Assert.That(decoded.Groups.Count).IsEqualTo(100_000);
    }

    /// <summary>Publication accepts exactly the maximum string length and reads it back.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishAcceptsMaximumStringLength(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-manifest-string-max");
        var store = new BootstrapManifestStore(dir);
        var groupId = new string('g', 4096);

        await store.PublishAsync(Manifest([new BootstrapGroupProgress(groupId, BootstrapGroupState.Pending)]), cancellationToken);
        var decoded = await store.ReadAsync(cancellationToken);

        _ = await Assert.That(decoded).IsNotNull();
        _ = await Assert.That(decoded.Groups[0].GroupId).IsEqualTo(groupId);
    }

    /// <summary>Publication rejects many valid groups whose combined payload exceeds the aggregate limit.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishRejectsAggregateOverMaximumSize(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-manifest-aggregate-over");
        var store = new BootstrapManifestStore(dir);
        var groups = new List<BootstrapGroupProgress>(5_000);
        for (var i = 0; i < 5_000; i++)
            groups.Add(new BootstrapGroupProgress(new string('g', 4096), BootstrapGroupState.Pending));

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(store.PublishAsync(Manifest(groups), cancellationToken));
    }

    /// <summary>Publication rejects source and target fingerprints that are not exactly 32 bytes.</summary>
    /// <param name="sourceLength">Source fingerprint length in bytes.</param>
    /// <param name="targetLength">Target fingerprint length in bytes.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(31, 32)]
    [Arguments(33, 32)]
    [Arguments(32, 31)]
    [Arguments(32, 33)]
    public async Task PublishRejectsNon32ByteFingerprints(int sourceLength, int targetLength, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-manifest-fingerprint-length");
        var store = new BootstrapManifestStore(dir);
        var manifest = Manifest([new BootstrapGroupProgress("group-1", BootstrapGroupState.Pending)], new byte[sourceLength], new byte[targetLength]);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(store.PublishAsync(manifest, cancellationToken));
    }

    /// <summary>Publication rejects group counts above the reader maximum before encoding.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishRejectsOverMaximumGroupCount(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-manifest-groups-over");
        var store = new BootstrapManifestStore(dir);
        var groups = new List<BootstrapGroupProgress>(100_001);
        for (var i = 0; i < 100_001; i++)
            groups.Add(new BootstrapGroupProgress($"group-{i}", BootstrapGroupState.Pending));

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(store.PublishAsync(Manifest(groups), cancellationToken));
    }

    /// <summary>Publication rejects group identifiers above the reader string limit before encoding.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishRejectsOverMaximumStringLength(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-manifest-string-over");
        var store = new BootstrapManifestStore(dir);
        var manifest = Manifest([new BootstrapGroupProgress(new string('g', 4097), BootstrapGroupState.Pending)]);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(store.PublishAsync(manifest, cancellationToken));
    }

    /// <summary>Reading a manifest beyond the size limit fails before allocating its contents.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadRejectsOversizedManifest(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-manifest-oversize-read");
        var store = new BootstrapManifestStore(dir);
        await store.PublishAsync(Manifest([new BootstrapGroupProgress("group-1", BootstrapGroupState.Pending)]), cancellationToken);

        using (var handle = File.OpenHandle(store.ManifestPath, FileMode.Open, FileAccess.Write, FileShare.None))
            RandomAccess.SetLength(handle, (16L * 1024 * 1024) + 1);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(store.ReadAsync(cancellationToken));
    }

    private static BootstrapManifest Manifest(IReadOnlyList<BootstrapGroupProgress> groups) => Manifest(groups, new byte[32], new byte[32]);

    private static BootstrapManifest Manifest(IReadOnlyList<BootstrapGroupProgress> groups, byte[] sourceFingerprint, byte[] targetFingerprint) => new()
    {
        Groups = groups,
        SourceClusterId = "cluster-a",
        SourceFingerprint = sourceFingerprint,
        SourceGeneration = 1UL,
        TargetFingerprint = targetFingerprint,
        TargetGeneration = 2UL,
        TargetReplicaCount = 3,
    };
}
