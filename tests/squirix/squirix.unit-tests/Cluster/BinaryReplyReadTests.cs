using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Internal.Cluster.Transport;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests.Cluster;

/// <summary>Checks ownership and framing of large distributed replies.</summary>
public sealed class BinaryReplyReadTests
{
    /// <summary>Both transports publish an independent array and leave the next frame unread.</summary>
    /// <param name="asyncRead">Whether to read through the asynchronous transport.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task LargeDistributedReplyOwnsItsPayload(bool asyncRead, CancellationToken cancellationToken)
    {
        const int payloadSize = 65536;
        var frame = new byte[payloadSize + 2];
        frame[0] = 1;
        frame[1] = 34;
        frame[^2] = 56;
        frame[^1] = 78;
        var scratch = new byte[1024];
        await using var stream = new MemoryStream(frame);

        var read = asyncRead
            ? await BinaryClientProtocol.ReadLargeLookupReplyAsync(stream, GetDistributedAsyncResponse.Parser, payloadSize + 1, scratch, cancellationToken)
            : BinaryClientProtocol.ReadBlockingReply(stream, GetDistributedAsyncResponse.Parser, 0, payloadSize + 1, scratch);
        var response = (read.Response as GetDistributedAsyncResponse)!;
        _ = await Assert.That(response.Found).IsTrue();
        _ = await Assert.That(MemoryMarshal.TryGetArray(response.Value, out var decoded)).IsTrue();
        var bytes = response.TakeValue();
        _ = await Assert.That(ReferenceEquals(bytes, decoded.Array)).IsTrue();
        _ = await Assert.That(bytes.Length).IsEqualTo(payloadSize);
        _ = await Assert.That(bytes.AsSpan().SequenceEqual(frame.AsSpan(1, payloadSize))).IsTrue();
        _ = await Assert.That(stream.ReadByte()).IsEqualTo(78);
    }

    /// <summary>Binary values are owned directly and non-binary replies retain their prefix and frame boundaries.</summary>
    /// <param name="binary">Whether the response contains a binary value.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task LargeLookupKeepsPayloadAndPrefix(bool binary, CancellationToken cancellationToken)
    {
        const int payloadLength = 65536;
        var frame = new byte[payloadLength + 2];
        frame[0] = binary ? byte.CreateChecked(31) : byte.CreateChecked(11);
        frame.AsSpan(1, payloadLength).Fill(120);
        frame[^1] = 29;
        await using var stream = new MemoryStream(frame);
        var scratch = new byte[1024];

        var read = await BinaryClientProtocol.ReadLargeLookupReplyAsync(stream, GetValueAsyncResponse.Parser, payloadLength + 1, scratch, cancellationToken);
        var response = await Assert.That(read.Response).IsTypeOf<GetValueAsyncResponse>();
        _ = await Assert.That(response!.Found).IsTrue();
        if (binary)
        {
            var wire = response.Value!;
            _ = await Assert.That(MemoryMarshal.TryGetArray(wire.BinaryValue, out var decoded)).IsTrue();
            var result = wire.TakeBinaryValue();
            _ = await Assert.That(ReferenceEquals(decoded.Array, result)).IsTrue();
            _ = await Assert.That(result.AsSpan().SequenceEqual(frame.AsSpan(1, payloadLength))).IsTrue();
        }
        else
        {
            _ = await Assert.That(response.Value!.StringValue).IsEqualTo(new string('x', payloadLength));
        }

        _ = await Assert.That(stream.ReadByte()).IsEqualTo(29);
    }

    /// <summary>A blocking reply whose caller has gone away is drained in scratch-size chunks.</summary>
    [Test]
    public async Task AbandonedReplyLeavesNextFrameReadable()
    {
        var frame = new byte[65537];
        frame[^1] = 91;
        await using var stream = new MemoryStream(frame);
        var scratch = new byte[1024];

        var result = BinaryClientProtocol.ReadBlockingReply(stream, null, 0, frame.Length - 1, scratch);

        _ = await Assert.That(result.Response).IsNull();
        _ = await Assert.That(stream.ReadByte()).IsEqualTo(91);
    }
}
