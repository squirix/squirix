using System;
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

/// <summary>Activated topology stamp persistence and boundary enforcement.</summary>
public sealed class ActivatedTopologyStampStoreTests : ServerUnitTestBase
{
    /// <summary>Mismatched generation, replica count, or fingerprint never matches.</summary>
    [Test]
    public async Task MatchesRejectsDifferences()
    {
        var stamp = new ActivatedTopologyStamp { Generation = 1, Fingerprint = new byte[32], ReplicaCount = 2 };

        _ = await Assert.That(stamp.Matches(new ActivatedTopologyStamp { Generation = 2, Fingerprint = new byte[32], ReplicaCount = 2 })).IsFalse();
        _ = await Assert.That(stamp.Matches(new ActivatedTopologyStamp { Generation = 1, Fingerprint = new byte[32], ReplicaCount = 3 })).IsFalse();
        _ = await Assert.That(stamp.Matches(new ActivatedTopologyStamp { Generation = 1, Fingerprint = new byte[] { 9, 8, 7 }, ReplicaCount = 2 })).IsFalse();
        _ = await Assert.That(stamp.Matches(new ActivatedTopologyStamp { Generation = 1, Fingerprint = new byte[32], ReplicaCount = 2 })).IsTrue();
    }

    /// <summary>Publication round-trips the stamped identity.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishReadsBackStamp(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-stamp-roundtrip");
        var store = new ActivatedTopologyStampStore(dir);
        var stamp = new ActivatedTopologyStamp { Generation = 2, Fingerprint = new byte[32], ReplicaCount = 3 };

        await store.PublishAsync(stamp, cancellationToken);
        var decoded = await store.ReadAsync(cancellationToken);

        _ = await Assert.That(decoded).IsNotNull();
        _ = await Assert.That(decoded.Matches(stamp)).IsTrue();
    }

    /// <summary>Publication rejects fingerprints that are not exactly 32 bytes.</summary>
    /// <param name="length">Fingerprint length in bytes.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(31)]
    [Arguments(33)]
    public async Task PublishRejectsNon32ByteFingerprint(int length, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-stamp-fingerprint-length");
        var store = new ActivatedTopologyStampStore(dir);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(
            store.PublishAsync(new ActivatedTopologyStamp { Generation = 1, Fingerprint = new byte[length], ReplicaCount = 2 }, cancellationToken));
    }

    /// <summary>A corrupted stamp file fails with InvalidDataException.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadCorruptThrowsInvalidData(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-stamp-corrupt");
        var store = new ActivatedTopologyStampStore(dir);
        await store.PublishAsync(new ActivatedTopologyStamp { Generation = 1, Fingerprint = new byte[32], ReplicaCount = 2 }, cancellationToken);
        var bytes = await File.ReadAllBytesAsync(store.StampPath, cancellationToken);
        bytes[^1] ^= 0xFF;
        await File.WriteAllBytesAsync(store.StampPath, bytes, cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(store.ReadAsync(cancellationToken));
    }

    /// <summary>Reading a directory that was never activated returns null.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadMissingReturnsNull(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-stamp-missing");
        var store = new ActivatedTopologyStampStore(dir);

        _ = await Assert.That(await store.ReadAsync(cancellationToken)).IsNull();
    }

    /// <summary>An oversized stamp file fails before its contents are allocated.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadOversizedThrowsInvalidData(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-stamp-oversize");
        var store = new ActivatedTopologyStampStore(dir);
        await store.PublishAsync(new ActivatedTopologyStamp { Generation = 1, Fingerprint = new byte[32], ReplicaCount = 2 }, cancellationToken);

        using (var handle = File.OpenHandle(store.StampPath, FileMode.Open, FileAccess.Write, FileShare.None))
            RandomAccess.SetLength(handle, 512);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(store.ReadAsync(cancellationToken));
    }
}
