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
