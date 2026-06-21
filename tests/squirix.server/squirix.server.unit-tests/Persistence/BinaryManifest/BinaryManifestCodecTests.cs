using System;
using System.IO;
using Squirix.Server.Storage.Manifest.Binary;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.BinaryManifest;

/// <summary>Round-trip tests for <see cref="BinaryManifestCodec" />.</summary>
public sealed class BinaryManifestCodecTests : UnitTestBase
{
    /// <summary>Verifies encode/decode round-trip for a manifest without snapshot metadata.</summary>
    [Fact]
    public void EncodeDecodeRoundTripsMinimalManifest()
    {
        var manifest = new Storage.Manifest.ManifestState { CurrentJournal = 2, NextSequence = 42 };

        var bytes = BinaryManifestCodec.Encode(manifest);
        var decoded = BinaryManifestCodec.Decode(bytes);

        Assert.Equal(manifest.Format, decoded.Format);
        Assert.Equal(manifest.CurrentJournal, decoded.CurrentJournal);
        Assert.Equal(manifest.NextSequence, decoded.NextSequence);
        Assert.Null(decoded.LastSnapshot);
    }

    /// <summary>Verifies encode/decode round-trip preserves snapshot reference fields.</summary>
    [Fact]
    public void EncodeDecodeRoundTripsSnapshotRef()
    {
        var created = new DateTime(2026, 6, 21, 12, 0, 0, DateTimeKind.Utc);
        var manifest = new Storage.Manifest.ManifestState
        {
            CurrentJournal = 5,
            NextSequence = 100,
            LastSnapshot = new Storage.Manifest.ManifestState.SnapshotRef
            {
                Index = 3,
                LastAppliedSequence = 99,
                ReplayFromJournalSegment = 2,
                CreatedUtc = created,
                Path = "/data/snp-000003.ssqx",
            },
        };

        var decoded = BinaryManifestCodec.Decode(BinaryManifestCodec.Encode(manifest));

        Assert.NotNull(decoded.LastSnapshot);
        Assert.Equal(manifest.LastSnapshot.Index, decoded.LastSnapshot!.Index);
        Assert.Equal(manifest.LastSnapshot.LastAppliedSequence, decoded.LastSnapshot.LastAppliedSequence);
        Assert.Equal(manifest.LastSnapshot.ReplayFromJournalSegment, decoded.LastSnapshot.ReplayFromJournalSegment);
        Assert.Equal(manifest.LastSnapshot.CreatedUtc, decoded.LastSnapshot.CreatedUtc);
        Assert.Equal(manifest.LastSnapshot.Path, decoded.LastSnapshot.Path);
    }

    /// <summary>Verifies roll-only encoding matches the general encoder for segment-roll updates.</summary>
    [Fact]
    public void WriteRollEncodedMatchesWriteEncodedWithoutSnapshot()
    {
        var manifest = new Storage.Manifest.ManifestState { CurrentJournal = 7, NextSequence = 99 };

        var expected = BinaryManifestCodec.Encode(manifest);
        Span<byte> roll = stackalloc byte[BinaryManifestCodec.RollEncodedWithoutSnapshotLength];
        var length = BinaryManifestCodec.WriteRollEncoded(manifest.Format, manifest.CurrentJournal, manifest.NextSequence, null, [], roll);

        Assert.Equal(expected.Length, length);
        Assert.True(expected.AsSpan().SequenceEqual(roll[..length]));
    }

    /// <summary>Verifies roll-only encoding matches the general encoder when snapshot metadata is present.</summary>
    [Fact]
    public void WriteRollEncodedMatchesWriteEncodedWithSnapshot()
    {
        var created = new DateTime(2026, 6, 21, 12, 0, 0, DateTimeKind.Utc);
        var manifest = new Storage.Manifest.ManifestState
        {
            CurrentJournal = 5,
            NextSequence = 100,
            LastSnapshot = new Storage.Manifest.ManifestState.SnapshotRef
            {
                Index = 3,
                LastAppliedSequence = 99,
                ReplayFromJournalSegment = 2,
                CreatedUtc = created,
                Path = "/data/snp-000003.ssqx",
            },
        };

        var expected = BinaryManifestCodec.Encode(manifest);
        var pathUtf8 = System.Text.Encoding.UTF8.GetBytes(manifest.LastSnapshot!.Path!);
        Span<byte> roll = stackalloc byte[expected.Length];
        var length = BinaryManifestCodec.WriteRollEncoded(
            manifest.Format,
            manifest.CurrentJournal,
            manifest.NextSequence,
            manifest.LastSnapshot,
            pathUtf8,
            roll);

        Assert.Equal(expected.Length, length);
        Assert.True(expected.AsSpan().SequenceEqual(roll[..length]));
    }

    /// <summary>Verifies corrupted CRC bytes are rejected on decode.</summary>
    [Fact]
    public void DecodeThrowsWhenCrcIsInvalid()
    {
        var bytes = BinaryManifestCodec.Encode(new Storage.Manifest.ManifestState());
        bytes[^1] ^= 0xFF;

        _ = Assert.Throws<InvalidDataException>(() => BinaryManifestCodec.Decode(bytes));
    }
}
