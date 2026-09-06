using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Node.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Replication;

/// <summary>Activated topology stamp persistence and boundary enforcement.</summary>
public sealed class ActivatedTopologyStampStoreTests : ServerUnitTestBase
{
    /// <summary>Mismatched generation, replica count, or fingerprint never matches.</summary>
    [Fact]
    public void MatchesRejectsDifferences()
    {
        var stamp = new ActivatedTopologyStamp { Generation = 1, Fingerprint = new byte[32], ReplicaCount = 2 };

        Assert.False(stamp.Matches(new ActivatedTopologyStamp { Generation = 2, Fingerprint = new byte[32], ReplicaCount = 2 }));
        Assert.False(stamp.Matches(new ActivatedTopologyStamp { Generation = 1, Fingerprint = new byte[32], ReplicaCount = 3 }));
        Assert.False(stamp.Matches(new ActivatedTopologyStamp { Generation = 1, Fingerprint = new byte[] { 9, 8, 7 }, ReplicaCount = 2 }));
        Assert.True(stamp.Matches(new ActivatedTopologyStamp { Generation = 1, Fingerprint = new byte[32], ReplicaCount = 2 }));
    }

    /// <summary>Publication round-trips the stamped identity.</summary>
    [Fact]
    public async Task PublishReadsBackStamp()
    {
        using var dir = new TempDirectory("squirix-stamp-roundtrip");
        var store = new ActivatedTopologyStampStore(dir);
        var stamp = new ActivatedTopologyStamp { Generation = 2, Fingerprint = new byte[32], ReplicaCount = 3 };

        await store.PublishAsync(stamp, DefaultCancellationToken);
        var decoded = await store.ReadAsync(DefaultCancellationToken);

        Assert.NotNull(decoded);
        Assert.True(decoded.Matches(stamp));
    }

    /// <summary>Publication rejects fingerprints that are not exactly 32 bytes.</summary>
    /// <param name="length">Fingerprint length in bytes.</param>
    [Theory]
    [InlineData(31)]
    [InlineData(33)]
    public async Task PublishRejectsNon32ByteFingerprint(int length)
    {
        using var dir = new TempDirectory("squirix-stamp-fingerprint-length");
        var store = new ActivatedTopologyStampStore(dir);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(
            store.PublishAsync(new ActivatedTopologyStamp { Generation = 1, Fingerprint = new byte[length], ReplicaCount = 2 }, DefaultCancellationToken));
    }

    /// <summary>A corrupted stamp file fails with InvalidDataException.</summary>
    [Fact]
    public async Task ReadCorruptThrowsInvalidData()
    {
        using var dir = new TempDirectory("squirix-stamp-corrupt");
        var store = new ActivatedTopologyStampStore(dir);
        await store.PublishAsync(new ActivatedTopologyStamp { Generation = 1, Fingerprint = new byte[32], ReplicaCount = 2 }, DefaultCancellationToken);
        var bytes = await File.ReadAllBytesAsync(store.StampPath, DefaultCancellationToken);
        bytes[^1] ^= 0xFF;
        await File.WriteAllBytesAsync(store.StampPath, bytes, DefaultCancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(store.ReadAsync(DefaultCancellationToken));
    }

    /// <summary>Reading a directory that was never activated returns null.</summary>
    [Fact]
    public async Task ReadMissingReturnsNull()
    {
        using var dir = new TempDirectory("squirix-stamp-missing");
        var store = new ActivatedTopologyStampStore(dir);

        Assert.Null(await store.ReadAsync(DefaultCancellationToken));
    }

    /// <summary>An oversized stamp file fails before its contents are allocated.</summary>
    [Fact]
    public async Task ReadOversizedThrowsInvalidData()
    {
        using var dir = new TempDirectory("squirix-stamp-oversize");
        var store = new ActivatedTopologyStampStore(dir);
        await store.PublishAsync(new ActivatedTopologyStamp { Generation = 1, Fingerprint = new byte[32], ReplicaCount = 2 }, DefaultCancellationToken);

        using (var handle = File.OpenHandle(store.StampPath, FileMode.Open, FileAccess.Write, FileShare.None))
            RandomAccess.SetLength(handle, 512);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(store.ReadAsync(DefaultCancellationToken));
    }
}
