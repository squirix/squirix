using System;
using System.Buffers.Binary;
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

        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(GroupLogFrameReader.ReadAllAsync(path, [(LogIndex, 0L)], cancellationToken));

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

        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(GroupLogFrameReader.ReadAllAsync(path, [(LogIndex, 0L)], cancellationToken));

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

        var entries = await GroupLogFrameReader.ReadAllAsync(path, [(LogIndex, 0L)], cancellationToken);

        _ = await Assert.That(entries.Length).IsEqualTo(1);
        _ = await Assert.That(entries[0].LogIndex).IsEqualTo(LogIndex);
    }

    private static byte[] Encode()
    {
        var entry = new FollowerLogEntry(LogIndex, 1UL, Payload);
        var buffer = new byte[GroupLogCodec.ComputeFrameEncodedLength(Payload.Length)];
        GroupLogCodec.EncodeFrame(buffer, in entry);
        return buffer;
    }
}
