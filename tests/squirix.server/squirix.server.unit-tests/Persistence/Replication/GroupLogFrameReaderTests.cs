using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Reading a committed frame back from a group log reports a frame that does not fit the file as corrupt.</summary>
[Immutable]
public sealed class GroupLogFrameReaderTests : ServerUnitTestBase
{
    private const ulong LogIndex = 1UL;

    /// <summary>Offset of the little-endian body length field in an encoded frame: magic(4) + version(1).</summary>
    private const int BodyLengthOffset = 5;

    /// <summary>A body length far beyond the end of the file.</summary>
    private const int OversizedBodyLength = 256 * 1024 * 1024;

    private static readonly byte[] Payload = [1, 2, 3, 4];

    /// <summary>A frame whose header is intact but whose tail is missing is corrupt.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TruncatedFrameIsCorrupt(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-group-log-frame-reader-truncated");
        var path = Path.Join(dir, "frame.log");
        var frame = Encode();
        await File.WriteAllBytesAsync(path, frame.AsMemory(0, frame.Length - 3), cancellationToken);
        (ulong LogIndex, long Offset)[] frames = [(LogIndex, 0L)];

        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(GroupLogFrameReader.ReadAllAsync(path, frames, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(ex.Message).Contains("corrupt", StringComparison.Ordinal);
    }

    /// <summary>A body length that claims far more bytes than the file holds is corrupt.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OversizedLengthFieldIsCorrupt(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-group-log-frame-reader-oversized");
        var path = Path.Join(dir, "frame.log");
        var frame = Encode();
        BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(BodyLengthOffset, sizeof(int)), OversizedBodyLength);
        await File.WriteAllBytesAsync(path, frame, cancellationToken);
        (ulong LogIndex, long Offset)[] frames = [(LogIndex, 0L)];

        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(GroupLogFrameReader.ReadAllAsync(path, frames, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(ex.Message).Contains("corrupt", StringComparison.Ordinal);
    }

    /// <summary>A frame that fits the file is read back unchanged.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task IntactFrameIsRead(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-group-log-frame-reader-intact");
        var path = Path.Join(dir, "frame.log");
        await File.WriteAllBytesAsync(path, Encode(), cancellationToken);
        (ulong LogIndex, long Offset)[] frames = [(LogIndex, 0L)];

        var entries = await GroupLogFrameReader.ReadAllAsync(path, frames, cancellationToken);

        _ = await Assert.That(entries.Length).IsEqualTo(1);
        _ = await Assert.That(entries[0].LogIndex).IsEqualTo(LogIndex);
    }

    /// <summary>Only the selected frame locations are read, including a segment with a nonzero offset.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OnlySelectedFramesAreRead(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-frame-selected-locations");
        var path = Path.Join(dir, "frame.log");
        await File.WriteAllBytesAsync(path, Encode(), cancellationToken);
        (ulong LogIndex, long Offset)[] frames = [(LogIndex + 1, 0L), (LogIndex, 0L), (LogIndex + 2, 0L)];

        var entries = await GroupLogFrameReader.ReadAllAsync(path, new ArraySegment<(ulong LogIndex, long Offset)>(frames, 1, 1), cancellationToken);

        _ = await Assert.That(entries.Length).IsEqualTo(1);
        _ = await Assert.That(entries[0].LogIndex).IsEqualTo(LogIndex);
        _ = await Assert.That(entries[0].Payload.Span.SequenceEqual(Payload)).IsTrue();
    }

    /// <summary>A visitor stopping after one entry never requests the next frame location.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EarlyStopDoesNotEnumerateMoreOffsets(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-frame-lazy-locations");
        var path = Path.Join(dir, "frame.log");
        await File.WriteAllBytesAsync(path, Encode(), cancellationToken);
        ulong visited = 0;

        var read = await GroupLogFrameReader.ReadAsync(
            path,
            FirstLocationOnly(),
            entry =>
            {
                visited = entry.LogIndex;
                return false;
            },
            cancellationToken);

        _ = await Assert.That((read, visited)).IsEqualTo((1, LogIndex));
    }

    /// <summary>Growing and shrinking frames preserve exact payloads after the whole batch has been read.</summary>
    /// <param name="useVisitor">Whether to read through the visitor overload.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task MixedSizesKeepOwnedPayloads(bool useVisitor, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-frame-mixed-sizes");
        var path = Path.Join(dir, "frames.log");
        var (bytes, locations, payloads) = BuildFrames();
        await File.WriteAllBytesAsync(path, bytes, cancellationToken);

        var entries = await ReadFramesAsync(path, locations, useVisitor, cancellationToken);

        _ = await Assert.That(entries.Length).IsEqualTo(payloads.Length);
        for (var i = 0; i < entries.Length; i++)
        {
            _ = await Assert.That((entries[i].LogIndex, entries[i].Term)).IsEqualTo((locations[i].LogIndex, 1UL));
            _ = await Assert.That(entries[i].Payload.Span.SequenceEqual(payloads[i])).IsTrue();
        }
    }

    /// <summary>A later short frame still rejects truncation, a bad CRC and an unexpected index.</summary>
    /// <param name="useVisitor">Whether to read through the visitor overload.</param>
    /// <param name="damage">The corruption kind.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false, 0)]
    [Arguments(false, 1)]
    [Arguments(false, 2)]
    [Arguments(true, 0)]
    [Arguments(true, 1)]
    [Arguments(true, 2)]
    public async Task LaterFrameCorruptionIsRejected(bool useVisitor, int damage, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-frame-later-corruption");
        var path = Path.Join(dir, "frames.log");
        var (bytes, locations, _) = BuildFrames();
        if (damage == 1)
            bytes[^1] ^= 0xFF;
        if (damage == 2)
            locations[^1] = (locations[^1].LogIndex + 1, locations[^1].Offset);
        await File.WriteAllBytesAsync(path, bytes.AsMemory(0, bytes.Length - (damage == 0 ? 1 : 0)), cancellationToken);

        var failure = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(ReadFramesAsync(path, locations, useVisitor, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(failure.Message).Contains("corrupt", StringComparison.Ordinal);
    }

    /// <summary>A short read after a prior large frame cannot decode stale bytes left in the buffer.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MidReadTruncationRejectsStaleBytes(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-frame-mid-read-truncation");
        var path = Path.Join(dir, "frames.log");
        var (bytes, locations, payloads) = BuildFrames();
        await File.WriteAllBytesAsync(path, bytes, cancellationToken);
        FollowerLogEntry first = default;

        var operation = GroupLogFrameReader.ReadAsync(
            path,
            locations,
            entry =>
            {
                first = entry;
                using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete);
                RandomAccess.SetLength(handle, locations[1].Offset + GroupLogCodec.FramePreambleByteCount + sizeof(int) + 1);
                return true;
            },
            cancellationToken);
        var failure = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(operation, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(failure.Message).Contains("corrupt", StringComparison.Ordinal);
        _ = await Assert.That(first.Payload.Span.SequenceEqual(payloads[0])).IsTrue();
    }

    /// <summary>A visitor failure preserves its exception and the payload already handed out.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VisitorFailureKeepsOwnedPayload(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-frame-visitor-failure");
        var path = Path.Join(dir, "frames.log");
        var (bytes, locations, payloads) = BuildFrames();
        await File.WriteAllBytesAsync(path, bytes, cancellationToken);
        var expected = new InvalidOperationException("Visitor failed.");
        FollowerLogEntry first = default;

        var operation = GroupLogFrameReader.ReadAsync(
            path,
            locations,
            entry =>
            {
                first = entry;
                throw expected;
            },
            cancellationToken);
        var failure = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(operation, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(ReferenceEquals(failure, expected)).IsTrue();
        _ = await Assert.That(first.Payload.Span.SequenceEqual(payloads[0])).IsTrue();
    }

    /// <summary>Cancellation between frames stops reading without changing the payload already handed out.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancellationKeepsOwnedPayload(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-frame-cancellation");
        var path = Path.Join(dir, "frames.log");
        var (bytes, locations, payloads) = BuildFrames();
        await File.WriteAllBytesAsync(path, bytes, cancellationToken);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        FollowerLogEntry first = default;

        var canceled = Task.CompletedTask;
        var operation = GroupLogFrameReader.ReadAsync(
            path,
            locations,
            entry =>
            {
                first = entry;
                canceled = cancellation.CancelAsync();
                return true;
            },
            cancellation.Token);
        var failure = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(operation, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        await canceled;

        _ = await Assert.That(failure.CancellationToken).IsEqualTo(cancellation.Token);
        _ = await Assert.That(first.Payload.Span.SequenceEqual(payloads[0])).IsTrue();
    }

    private static (byte[] Bytes, (ulong LogIndex, long Offset)[] Locations, byte[][] Payloads) BuildFrames()
    {
        int[] lengths = [4096, 3, 16_384, 7];
        var payloads = new byte[lengths.Length][];
        var locations = new (ulong LogIndex, long Offset)[lengths.Length];
        var totalLength = 0;
        for (var i = 0; i < lengths.Length; i++)
        {
            payloads[i] = new byte[lengths[i]];
            Array.Fill(payloads[i], byte.CreateChecked(i + 1));
            locations[i] = (ulong.CreateChecked(i + 1), totalLength);
            totalLength += GroupLogCodec.ComputeFrameEncodedLength(lengths[i]);
        }

        var bytes = new byte[totalLength];
        for (var i = 0; i < payloads.Length; i++)
        {
            var entry = new FollowerLogEntry(locations[i].LogIndex, 1UL, payloads[i]);
            GroupLogCodec.EncodeFrame(bytes.AsSpan(int.CreateChecked(locations[i].Offset), GroupLogCodec.ComputeFrameEncodedLength(payloads[i].Length)), in entry);
        }

        return (bytes, locations, payloads);
    }

    private static async Task<FollowerLogEntry[]> ReadFramesAsync(string path, (ulong LogIndex, long Offset)[] locations, bool useVisitor, CancellationToken cancellationToken)
    {
        if (!useVisitor)
            return await GroupLogFrameReader.ReadAllAsync(path, locations, cancellationToken);

        var entries = new List<FollowerLogEntry>();
        var read = await GroupLogFrameReader.ReadAsync(
            path,
            locations,
            entry =>
            {
                entries.Add(entry);
                return true;
            },
            cancellationToken);
        _ = await Assert.That(read).IsEqualTo(entries.Count);
        return [.. entries];
    }

    private static IEnumerable<(ulong LogIndex, long Offset)> FirstLocationOnly()
    {
        yield return (LogIndex, 0L);
        throw new InvalidOperationException("Unvisited frame location requested.");
    }

    private static byte[] Encode()
    {
        var entry = new FollowerLogEntry(LogIndex, 1UL, Payload);
        var buffer = new byte[GroupLogCodec.ComputeFrameEncodedLength(Payload.Length)];
        GroupLogCodec.EncodeFrame(buffer, in entry);
        return buffer;
    }
}
