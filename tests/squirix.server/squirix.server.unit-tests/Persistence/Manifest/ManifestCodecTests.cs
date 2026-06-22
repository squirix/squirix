using System;
using System.IO;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>Round-trip tests for <see cref="ManifestCodec" />.</summary>
public sealed class ManifestCodecTests : UnitTestBase
{
    /// <summary>Verifies encode/decode round-trip for a manifest without snapshot metadata.</summary>
    [Fact]
    public void EncodeDecodeRoundTripsMinimalManifest()
    {
        var manifest = new ManifestState { CurrentJournal = 2, NextSequence = 42 };

        var bytes = ManifestCodec.Encode(manifest);
        var decoded = ManifestCodec.Decode(bytes);

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
        var manifest = new ManifestState
        {
            CurrentJournal = 5,
            NextSequence = 100,
            LastSnapshot = new ManifestState.SnapshotRef
            {
                Index = 3,
                LastAppliedSequence = 99,
                ReplayFromJournalSegment = 2,
                CreatedUtc = created,
                Path = "/data/snp-000003.bsqx",
            },
        };

        var decoded = ManifestCodec.Decode(ManifestCodec.Encode(manifest));

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
        var manifest = new ManifestState { CurrentJournal = 7, NextSequence = 99 };

        var expected = ManifestCodec.Encode(manifest);
        Span<byte> roll = stackalloc byte[ManifestCodec.RollEncodedWithoutSnapshotLength];
        var length = ManifestCodec.WriteRollEncoded(manifest.Format, manifest.CurrentJournal, manifest.NextSequence, null, [], roll);

        Assert.Equal(expected.Length, length);
        Assert.True(expected.AsSpan().SequenceEqual(roll[..length]));
    }

    /// <summary>Verifies roll-only encoding matches the general encoder when snapshot metadata is present.</summary>
    [Fact]
    public void WriteRollEncodedMatchesWriteEncodedWithSnapshot()
    {
        var created = new DateTime(2026, 6, 21, 12, 0, 0, DateTimeKind.Utc);
        var manifest = new ManifestState
        {
            CurrentJournal = 5,
            NextSequence = 100,
            LastSnapshot = new ManifestState.SnapshotRef
            {
                Index = 3,
                LastAppliedSequence = 99,
                ReplayFromJournalSegment = 2,
                CreatedUtc = created,
                Path = "/data/snp-000003.bsqx",
            },
        };

        var expected = ManifestCodec.Encode(manifest);
        var pathUtf8 = System.Text.Encoding.UTF8.GetBytes(manifest.LastSnapshot!.Path!);
        Span<byte> roll = stackalloc byte[expected.Length];
        var length = ManifestCodec.WriteRollEncoded(
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
        var bytes = ManifestCodec.Encode(new ManifestState());
        bytes[^1] ^= 0xFF;

        _ = Assert.Throws<InvalidDataException>(() => ManifestCodec.Decode(bytes));
    }
}
