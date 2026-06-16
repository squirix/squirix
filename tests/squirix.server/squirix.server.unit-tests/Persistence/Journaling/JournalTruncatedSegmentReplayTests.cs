using System;
using System.Collections.Generic;
using System.IO;
using Google.Protobuf;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Json;
using Squirix.Server.Storage.JournalProto;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>
/// Replay behavior when journal segment bytes end mid-frame or fail CRC / protobuf decode.
/// </summary>
public sealed class JournalTruncatedSegmentReplayTests : UnitTestBase
{
    /// <summary>
    /// Verifies replay failure reporting is non-destructive: reading malformed frames does not mutate segment bytes.
    /// </summary>
    [Fact]
    public void ReadAllOnMalformedFrameDoesNotMutateSegmentFile()
    {
        using var dir = new TempDirectory("squirix-journal-readonly-failure");
        var env = BuildPutEnvelope(1UL, "k", "v");
        var path = PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}000001{StorageFileExtensions.Journal}");
        WriteSegmentWithFrames(path, [env]);

        var original = File.ReadAllBytes(path);
        var bytes = (byte[])original.Clone();
        bytes[^1] ^= 0xFF;
        File.WriteAllBytes(path, bytes);
        var mutatedBeforeRead = File.ReadAllBytes(path);

        _ = Assert.Throws<InvalidDataException>(() =>
        {
            foreach (var unused in JournalReader.ReadAll(dir, 1, DefaultCancellationToken))
                _ = unused;
        });
        Assert.Equal(mutatedBeforeRead, File.ReadAllBytes(path));
    }

    /// <summary>
    /// CRC mismatch throws <see cref="InvalidDataException" /> to surface corruption.
    /// </summary>
    [Fact]
    public void ReadAllThrowsOnCrcMismatch()
    {
        using var dir = new TempDirectory("squirix-journal-badcrc");
        var env = BuildPutEnvelope(1UL, "k", "v");
        var path = PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}000001{StorageFileExtensions.Journal}");
        WriteSegmentWithFrames(path, [env]);

        var bytes = File.ReadAllBytes(path);
        bytes[^1] ^= 0xFF;
        File.WriteAllBytes(path, bytes);

        var ex = Assert.Throws<InvalidDataException>(() =>
        {
            foreach (var unused in JournalReader.ReadAll(dir, 1, DefaultCancellationToken))
            {
                _ = unused;
            }
        });
        Assert.Contains("ChecksumMismatch", ex.Message, StringComparison.InvariantCulture);
    }

    /// <summary>
    /// Verifies the first complete frame is yielded and enumeration stops when a trailing frame is torn (CRC no longer matches).
    /// </summary>
    [Fact]
    public void ReadAllYieldsFirstFrameWhenSecondFrameCrcIsTruncated()
    {
        using var dir = new TempDirectory("squirix-journal-trunc");
        var first = BuildPutEnvelope(1UL, "k1", "a");
        var second = BuildPutEnvelope(2UL, "k2", "b");
        var path = PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}000001{StorageFileExtensions.Journal}");
        WriteSegmentWithFrames(path, [first, second]);

        using (var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
        {
            fs.SetLength(fs.Length - 1);
        }

        var list = new List<JournalEnvelope>();
        foreach (var e in JournalReader.ReadAll(dir, 1, DefaultCancellationToken))
            list.Add(e);

        _ = Assert.Single(list);
        Assert.Equal(JournalEnvelope.OpOneofCase.Put, list[0].OpCase);
        Assert.Equal("k1", list[0].Put.Item.Key);
    }

    private static JournalEnvelope BuildPutEnvelope(ulong seq, string key, string value)
    {
        var body = DiscriminatedEntryJsonWriter.BuildEntryJson(value, null, null, 1, null);
        return new JournalEnvelope
        {
            Seq = seq,
            UnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Put = new Put
            {
                Item = new EntryPair
                {
                    Key = key,
                    Namespace = CacheNames.DefaultNamespace,
                    EntryJson = ByteString.CopyFrom(body),
                },
            },
        };
    }

    private static void WriteSegmentWithFrames(string path, IReadOnlyList<JournalEnvelope> envelopes)
    {
        using var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read);
        JournalFraming.WriteFileHeader(stream);
        foreach (var envelope in envelopes)
        {
            var payload = RecordCodec.Serialize(envelope);
            JournalFraming.WriteFrame(stream, payload);
        }

        stream.Flush(true);
    }
}
