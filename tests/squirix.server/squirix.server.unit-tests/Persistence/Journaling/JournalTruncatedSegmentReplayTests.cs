using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.JsonFramed;
using Squirix.Server.Storage.Journaling.JsonFramed.Json;
using Squirix.Server.Storage.JournalProto;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Replay behavior when journal segment bytes end mid-frame or fail CRC / protobuf decode.</summary>
public sealed class JournalTruncatedSegmentReplayTests : UnitTestBase
{
    /// <summary>Verifies replay failure reporting is non-destructive: reading malformed frames does not mutate segment bytes.</summary>
    [Fact]
    public async Task ReadAllOnMalformedFrameDoesNotMutateSegmentFile()
    {
        using var dir = new TempDirectory("squirix-journal-readonly-failure");
        var env = await BuildPutEnvelopeAsync(1UL, "k", "v");
        var path = PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}000001{StorageFileExtensions.Journal}");
        await WriteSegmentWithFramesAsync(path, [env]);

        var original = await File.ReadAllBytesAsync(path, DefaultCancellationToken);
        var bytes = new byte[original.Length];
        Array.Copy(original, bytes, original.Length);
        bytes[^1] ^= 0xFF;
        await File.WriteAllBytesAsync(path, bytes, DefaultCancellationToken);
        var mutatedBeforeRead = await File.ReadAllBytesAsync(path, DefaultCancellationToken);

        _ = Assert.Throws<InvalidDataException>(() =>
        {
            foreach (var unused in JournalReader.ReadAll(dir, 1, DefaultCancellationToken))
                _ = unused;
        });
        Assert.Equal(mutatedBeforeRead, await File.ReadAllBytesAsync(path, DefaultCancellationToken));
    }

    /// <summary>
    /// CRC mismatch throws <see cref="InvalidDataException" /> to surface corruption.
    /// </summary>
    [Fact]
    public async Task ReadAllThrowsOnCrcMismatch()
    {
        using var dir = new TempDirectory("squirix-journal-badcrc");
        var env = await BuildPutEnvelopeAsync(1UL, "k", "v");
        var path = PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}000001{StorageFileExtensions.Journal}");
        await WriteSegmentWithFramesAsync(path, [env]);

        var bytes = await File.ReadAllBytesAsync(path, DefaultCancellationToken);
        bytes[^1] ^= 0xFF;
        await File.WriteAllBytesAsync(path, bytes, DefaultCancellationToken);

        var ex = Assert.Throws<InvalidDataException>(() =>
        {
            foreach (var unused in JournalReader.ReadAll(dir, 1, DefaultCancellationToken))
            {
                _ = unused;
            }
        });
        Assert.Contains("ChecksumMismatch", ex.Message, StringComparison.InvariantCulture);
    }

    /// <summary>Verifies the first complete frame is yielded and enumeration stops when a trailing frame is torn (CRC no longer matches).</summary>
    [Fact]
    public async Task ReadAllYieldsFirstFrameWhenSecondFrameCrcIsTruncated()
    {
        using var dir = new TempDirectory("squirix-journal-trunc");
        var first = await BuildPutEnvelopeAsync(1UL, "k1", "a");
        var second = await BuildPutEnvelopeAsync(2UL, "k2", "b");
        var path = PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}000001{StorageFileExtensions.Journal}");
        await WriteSegmentWithFramesAsync(path, [first, second]);

        await using (var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
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

    private static async Task<JournalEnvelope> BuildPutEnvelopeAsync(ulong seq, string key, string value)
    {
        var body = await DiscriminatedEntryJsonWriter.BuildEntryJsonAsync(value, null, null, 1, null);
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

    private static async Task WriteSegmentWithFramesAsync(string path, IReadOnlyList<JournalEnvelope> envelopes)
    {
        await using var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read);
        JournalFraming.WriteFileHeader(stream);
        foreach (var envelope in envelopes)
        {
            var payload = RecordCodec.Serialize(envelope);
            JournalFraming.WriteFrame(stream, payload);
        }

        await stream.FlushAsync(DefaultCancellationToken);
    }
}
