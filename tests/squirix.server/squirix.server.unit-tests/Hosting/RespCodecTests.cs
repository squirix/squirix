using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint.Resp;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies the RESP wire limits and preserves byte-level data through the codec.</summary>
public sealed class RespCodecTests
{
    /// <summary>One-byte reads preserve binary payloads and frame boundaries.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadsFragmentedBinaryRequests(CancellationToken cancellationToken)
    {
        byte[] payload = [0, 255, 13, 10];
        byte[] wire = [.. "*2\r\n$4\r\nECHO\r\n$4\r\n"u8, .. payload, .. "\r\n*1\r\n$4\r\nPING\r\n"u8];
        await using var stream = new FragmentedStream(wire);
        var reader = new RequestReader();

        _ = await Assert.That(await reader.ReadAsync(stream, cancellationToken)).IsTrue();
        _ = await Assert.That(reader.Count).IsEqualTo(2);
        _ = await Assert.That(reader[0].Span.SequenceEqual("ECHO"u8)).IsTrue();
        _ = await Assert.That(reader[1].Span.SequenceEqual(payload)).IsTrue();

        _ = await Assert.That(await reader.ReadAsync(stream, cancellationToken)).IsTrue();
        _ = await Assert.That(reader.Count).IsEqualTo(1);
        _ = await Assert.That(reader[0].Span.SequenceEqual("PING"u8)).IsTrue();
        _ = await Assert.That(await reader.ReadAsync(stream, cancellationToken)).IsFalse();
    }

    /// <summary>One read can contain multiple complete requests, including an empty bulk argument.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadsCoalescedFramesInOrder(CancellationToken cancellationToken)
    {
        await using var stream = new MemoryStream([.. "*2\r\n$4\r\nECHO\r\n$0\r\n\r\n*1\r\n$4\r\nPING\r\n"u8]);
        var reader = new RequestReader();

        _ = await Assert.That(await reader.ReadAsync(stream, cancellationToken)).IsTrue();
        _ = await Assert.That(reader.Count).IsEqualTo(2);
        _ = await Assert.That(reader[1].IsEmpty).IsTrue();
        _ = await Assert.That(stream.Position).IsEqualTo(stream.Length);
        _ = await Assert.That(await reader.ReadAsync(stream, cancellationToken)).IsTrue();
        _ = await Assert.That(reader[0].Span.SequenceEqual("PING"u8)).IsTrue();
        _ = await Assert.That(await reader.ReadAsync(stream, cancellationToken)).IsFalse();
    }

    /// <summary>Closes the input on invalid or oversized framing before consuming the declared payload.</summary>
    /// <param name="wire">Malformed request bytes as ASCII.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments("*9\r\n")]
    [Arguments("*1\r\n$1048577\r\n")]
    [Arguments("*2\r\n$-1\r\n")]
    [Arguments("*1\r\n$1\r\nx!!")]
    [Arguments("*1\r\n$5\r\nx")]
    [Arguments("*00000001\r\n")]
    [Arguments("*0\r\n")]
    [Arguments("PING\r\n")]
    public async Task RejectsInvalidFraming(string wire, CancellationToken cancellationToken)
    {
        await using var stream = new MemoryStream(System.Text.Encoding.ASCII.GetBytes(wire));
        var reader = new RequestReader();

        var error = await NodeAsyncAssert.ThrowsAsync<InvalidDataException, bool>(reader.ReadAsync(stream, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(error.Message.Length).IsGreaterThan(0);
    }

    /// <summary>Accepts the maximum bulk length and rejects a frame whose later valid bulk would exceed the total limit.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EnforcesFrameLimitAcrossArguments(CancellationToken cancellationToken)
    {
        await using var stream = new MemoryStream();
        stream.Write("*2\r\n$1048576\r\n"u8);
        stream.SetLength(stream.Length + RequestReader.MaxBulkBytes);
        stream.Position = stream.Length;
        stream.Write("\r\n$16384\r\n"u8);
        stream.Position = 0;
        var reader = new RequestReader();

        var error = await NodeAsyncAssert.ThrowsAsync<InvalidDataException, bool>(reader.ReadAsync(stream, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(error.Message).IsEqualTo("RESP request exceeds the frame limit.");
    }

    /// <summary>Writes version-specific nulls, an integer and an opaque bulk value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WritesWireReplies(CancellationToken cancellationToken)
    {
        await using var stream = new MemoryStream();
        await Reply.WriteNullAsync(stream, false, cancellationToken);
        await Reply.WriteNullAsync(stream, true, cancellationToken);
        await Reply.WriteIntegerAsync(stream, long.MinValue, cancellationToken);
        await Reply.WriteBulkAsync(stream, new byte[] { 0, 255 }, cancellationToken);

        byte[] expected = [.. "$-1\r\n_\r\n:-9223372036854775808\r\n$2\r\n"u8, 0, 255, .. "\r\n"u8];
        _ = await Assert.That(stream.ToArray().AsSpan().SequenceEqual(expected)).IsTrue();
    }

    private sealed class FragmentedStream : MemoryStream
    {
        internal FragmentedStream(byte[] bytes)
            : base(bytes)
        {
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            base.ReadAsync(buffer[..1], cancellationToken);
    }
}
