using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Codec;

/// <summary>Verifies the windowed frame reader yields exactly the results of the per-frame reader for every file layout.</summary>
[Immutable]
public sealed class JournalFrameWindowReaderTests : ServerUnitTestBase
{
    private const int EdgeBytes = 12;
    private const int LargeFrameStep = 251;
    private const int LargePayloadBytes = 5_000;

    private static ReadOnlySpan<int> WindowSizes => [JournalFrameEnvelope.HeaderSize, 7, 64, 1_024, JournalFrameWindowReader.DefaultWindowSize];

    /// <summary>Verifies a flipped checksum byte and a flipped payload byte classify like the per-frame reader.</summary>
    [Test]
    public async Task CorruptedFramesMatchReference()
    {
        var bytes = BuildFrames(10, 40, 3, 70);
        var secondFrameStart = JournalFrameEnvelope.TotalLength(10);
        var secondFrameEnd = secondFrameStart + JournalFrameEnvelope.TotalLength(40);
        byte[] crcFlipped = [.. bytes];
        crcFlipped[secondFrameEnd - 1] ^= 0xFF;
        byte[] payloadFlipped = [.. bytes];
        payloadFlipped[secondFrameStart + JournalFrameEnvelope.HeaderSize] ^= 0x01;

        _ = await Assert.That(await CompareAllAsync(crcFlipped, crcFlipped.Length)).IsEqualTo(string.Empty);
        _ = await Assert.That(await CompareAllAsync(payloadFlipped, payloadFlipped.Length)).IsEqualTo(string.Empty);
    }

    /// <summary>Verifies an empty file and files shorter than a frame header match the per-frame reader.</summary>
    [Test]
    public async Task EmptyAndHeaderOnlyFilesMatchReference()
    {
        var bytes = BuildFrames(10);
        for (var length = 0; length <= JournalFrameEnvelope.HeaderSize; length++)
            _ = await Assert.That(await CompareAllAsync(bytes, length)).IsEqualTo(string.Empty);
    }

    /// <summary>Verifies an oversized or negative declared length matches the per-frame reader.</summary>
    [Test]
    public async Task OversizedLengthMatchesReference()
    {
        var valid = BuildFrames(10, 20);
        foreach (var declared in new[] { int.MaxValue, JournalSegmentLimits.MaxFramePayloadBytes + 1, -1, int.MinValue })
        {
            var bytes = new byte[valid.Length + 16];
            valid.CopyTo(bytes, 0);
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(valid.Length), declared);
            _ = await Assert.That(await CompareAllAsync(bytes, bytes.Length)).IsEqualTo(string.Empty);
        }
    }

    /// <summary>Verifies a file cut at every byte of its last frame matches the per-frame reader, including a file that is longer than the known length.</summary>
    [Test]
    public async Task TruncationAtEveryByteMatchesReference()
    {
        var bytes = BuildFrames(10, 40, 3, 100);
        var lastFrameStart = bytes.Length - JournalFrameEnvelope.TotalLength(100);
        for (var length = lastFrameStart; length <= bytes.Length; length++)
            _ = await Assert.That(await CompareAllAsync(bytes, length)).IsEqualTo(string.Empty);
    }

    /// <summary>Verifies a file cut at every byte of a frame larger than the windows matches the per-frame reader.</summary>
    [Test]
    public async Task LargeFrameTruncationMatchesReference()
    {
        var bytes = BuildFrames(10, LargePayloadBytes);
        var largeFrameStart = JournalFrameEnvelope.TotalLength(10);
        for (var length = largeFrameStart; length <= bytes.Length; length++)
        {
            var inFrame = length - largeFrameStart;
            var isEdge = inFrame <= EdgeBytes || bytes.Length - length <= EdgeBytes;
            if (isEdge || inFrame % LargeFrameStep == 0)
                _ = await Assert.That(await CompareAllAsync(bytes, length)).IsEqualTo(string.Empty);
        }
    }

    /// <summary>Verifies valid files with frames crossing window boundaries and a frame larger than the window match the per-frame reader.</summary>
    [Test]
    public async Task ValidFilesMatchReference()
    {
        var layouts = new[]
        {
            BuildFrames(1),
            BuildFrames(0, 0, 0),
            BuildFrames(1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233),
            BuildFrames(10, LargePayloadBytes, 10, 300, LargePayloadBytes * 3, 1),
            BuildFrames(Repeat(17, 500)),
        };
        foreach (var bytes in layouts)
            _ = await Assert.That(await CompareAllAsync(bytes, bytes.Length)).IsEqualTo(string.Empty);
    }

    private static byte[] BuildFrames(params int[] payloadLengths)
    {
        var total = 0;
        foreach (var payloadLength in payloadLengths)
            total += JournalFrameEnvelope.TotalLength(payloadLength);

        var bytes = new byte[total];
        var offset = 0;
        var seed = 1;
        foreach (var payloadLength in payloadLengths)
        {
            var frame = bytes.AsSpan(offset, JournalFrameEnvelope.TotalLength(payloadLength));
            var payload = frame.Slice(JournalFrameEnvelope.HeaderSize, payloadLength);
            for (var i = 0; i < payload.Length; i++)
                payload[i] = byte.CreateTruncating(seed + (i * 31));

            JournalFraming.SealFrame(frame);
            offset += frame.Length;
            seed++;
        }

        return bytes;
    }

    /// <summary>Compares both readers over the first <paramref name="visibleLength" /> bytes with every window size.</summary>
    /// <param name="bytes">The full file content.</param>
    /// <param name="visibleLength">The number of leading bytes that exist; the windowed reader is given the full file with this known length.</param>
    /// <returns>An empty string when the sequences match, otherwise a description of the first difference.</returns>
    private static async Task<string> CompareAllAsync(byte[] bytes, int visibleLength)
    {
        var cutPath = await WriteTempAsync(bytes[..visibleLength]);
        var fullPath = await WriteTempAsync(bytes);
        try
        {
            return CompareWindows(cutPath, fullPath, visibleLength);
        }
        finally
        {
            File.Delete(cutPath);
            File.Delete(fullPath);
        }
    }

    private static string CompareWindows(string cutPath, string fullPath, int visibleLength)
    {
        using var cutHandle = OpenRead(cutPath);
        using var fullHandle = OpenRead(fullPath);
        var expected = ReadWithPerFrameReader(cutHandle);
        foreach (var windowSize in WindowSizes)
        {
            var actual = ReadWithWindowReader(fullHandle, visibleLength, windowSize);
            if (expected.Count != actual.Count)
                return $"length {visibleLength} window {windowSize}: {expected.Count} results expected, {actual.Count} read.";

            for (var i = 0; i < expected.Count; i++)
            {
                if (!string.Equals(expected[i], actual[i], StringComparison.Ordinal))
                    return $"length {visibleLength} window {windowSize} result {i}: expected '{expected[i]}' but read '{actual[i]}'.";
            }
        }

        return string.Empty;
    }

    private static SafeFileHandle OpenRead(string path) => File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete, FileOptions.SequentialScan);

    private static string Describe(JournalFrameReadResult result, int payloadLength, ReadOnlySpan<byte> payload) =>
        $"{result.Status}|{result.NextFrameOffset}|{payloadLength}|{Convert.ToHexString(payload)}";

    private static List<string> ReadWithPerFrameReader(SafeFileHandle handle)
    {
        var results = new List<string>();
        long offset = 0;
        var fileLength = RandomAccess.GetLength(handle);
        while (true)
        {
            var result = JournalFrameReader.ReadNext(handle, offset, fileLength, out var buffer, out var payloadLength);
            results.Add(Describe(result, payloadLength, buffer == null ? [] : buffer.AsSpan(0, payloadLength)));
            if (buffer != null)
                ArrayPool<byte>.Shared.Return(buffer);

            if (result.Status != JournalFrameReadStatus.Success)
                return results;

            offset = result.NextFrameOffset;
        }
    }

    private static List<string> ReadWithWindowReader(SafeFileHandle handle, long fileLength, int windowSize)
    {
        var results = new List<string>();
        using var reader = new JournalFrameWindowReader(handle, fileLength, windowSize);
        long offset = 0;
        while (true)
        {
            var result = reader.ReadNext(offset, out var buffer, out var payloadLength);
            results.Add(Describe(result, payloadLength, buffer == null ? [] : buffer.AsSpan(0, payloadLength)));
            if (result.Status != JournalFrameReadStatus.Success)
                return results;

            offset = result.NextFrameOffset;
        }
    }

    private static int[] Repeat(int value, int count)
    {
        var values = new int[count];
        Array.Fill(values, value);
        return values;
    }

    private static async Task<string> WriteTempAsync(byte[] bytes)
    {
        var path = Path.Join(Path.GetTempPath(), "jfw-" + Guid.NewGuid().ToString("N"));
        await File.WriteAllBytesAsync(path, bytes, CancellationToken.None);
        return path;
    }
}
