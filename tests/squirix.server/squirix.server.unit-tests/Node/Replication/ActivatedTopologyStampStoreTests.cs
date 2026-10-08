using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Node.Replication;
using Squirix.Server.Storage.Journaling.Abstractions;
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
    /// <summary>Matching identities describe no change.</summary>
    [Test]
    public async Task DescribeChangeEmptyWhenMatching()
    {
        var description = Stamp(1, 2, 0x5A).DescribeChange(Stamp(1, 2, 0x5A));

        _ = await Assert.That(description).IsEmpty();
    }

    /// <summary>A fingerprint-only change is named with both hex digests and the inputs that can cause it.</summary>
    [Test]
    public async Task DescribeChangeNamesFingerprintOnly()
    {
        var description = Stamp(4, 3, 0xAB).DescribeChange(Stamp(4, 3, 0x0C));

        _ = await Assert.That(description).IsEqualTo(
            $"topology fingerprint changed (stamped {Hex(0xAB)}, configured {Hex(0x0C)}) while generation and replica count match, " +
            "so the cluster id, virtual nodes, peers (including internode addresses), minimum cluster package version, replication policy constants, " +
            "or the automatic failover and quorum read switches differ");
    }

    /// <summary>A generation change is named with both values; the fingerprint that hashes it is not.</summary>
    [Test]
    public async Task DescribeChangeNamesGeneration()
    {
        var description = Stamp(1, 2, 0x00).DescribeChange(Stamp(2, 2, 0x01));

        _ = await Assert.That(description).IsEqualTo("generation changed (stamped 1, configured 2)");
    }

    /// <summary>Generation and replica count changes are both named in field order.</summary>
    [Test]
    public async Task DescribeChangeNamesGenerationAndCount()
    {
        var description = Stamp(1, 2, 0x00).DescribeChange(Stamp(3, 3, 0x01));

        _ = await Assert.That(description).IsEqualTo("generation changed (stamped 1, configured 3); replica count changed (stamped 2, configured 3)");
    }

    /// <summary>A replica count change is named with both values; the fingerprint that hashes it is not.</summary>
    [Test]
    public async Task DescribeChangeNamesReplicaCount()
    {
        var description = Stamp(1, 2, 0x00).DescribeChange(Stamp(1, 3, 0x01));

        _ = await Assert.That(description).IsEqualTo("replica count changed (stamped 2, configured 3)");
    }

    /// <summary>Only journal segment files count as durable cache journal state; the stamp and other files do not.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task JournalStateDetectsSegmentFiles(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-stamp-journal-state");
        await new ActivatedTopologyStampStore(dir).PublishAsync(
            new ActivatedTopologyStamp { Generation = 1, Fingerprint = new byte[32], ReplicaCount = 2 },
            cancellationToken);
        await File.WriteAllBytesAsync(Path.Join(dir, "notes.txt"), [1], cancellationToken);

        _ = await Assert.That(ActivatedTopologyStampStore.HasDurableCacheJournalState(dir)).IsFalse();

        await File.WriteAllBytesAsync(Path.Join(dir, $"{FilePrefixes.Journal}000001{FileExtensions.Journal}"), [1], cancellationToken);

        _ = await Assert.That(ActivatedTopologyStampStore.HasDurableCacheJournalState(dir)).IsTrue();
    }

    /// <summary>A data directory that does not exist yet holds no journal state.</summary>
    [Test]
    public async Task JournalStateFalseForMissingDirectory()
    {
        using var dir = new TempDirectory("squirix-stamp-journal-missing");

        _ = await Assert.That(ActivatedTopologyStampStore.HasDurableCacheJournalState(Path.Join(dir, "absent"))).IsFalse();
    }

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

    private static byte[] Filled(byte value)
    {
        var fingerprint = GC.AllocateUninitializedArray<byte>(32);
        fingerprint.AsSpan().Fill(value);
        return fingerprint;
    }

    private static string Hex(byte value) => Convert.ToHexString(Filled(value));

    private static ActivatedTopologyStamp Stamp(ulong generation, int replicaCount, byte fingerprint) =>
        new() { Generation = generation, Fingerprint = Filled(fingerprint), ReplicaCount = replicaCount };
}
