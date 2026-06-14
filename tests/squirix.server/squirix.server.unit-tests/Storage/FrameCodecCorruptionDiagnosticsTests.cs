using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Utils;
using Xunit;

namespace Squirix.Server.UnitTests.Storage;

/// <summary>
/// Tests CRC mismatch diagnostics on framed snapshot reads.
/// </summary>
public sealed class FrameCodecCorruptionDiagnosticsTests
{
    /// <summary>
    /// Verifies strict frame reads include stable lowercase CRC hex values in the exception message.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when assertions pass.</returns>
    [Fact]
    public async Task ReadFrameStrictAsyncReportsLowerHexCrcValuesOnMismatch()
    {
        const string expectedCrcHex = "7ab201a3";
        const string wrongCrcHex = "854dfe5c";
        var payload = "{\"k\":1}"u8.ToArray();
        var expectedOnWire = Crc32C.Compute(payload);
        var wrongCrc = expectedOnWire ^ 0xFFFF_FFFFu;

        await using var ms = new MemoryStream();
        var header = new byte[8];
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(0, 4), (uint)payload.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(4, 4), wrongCrc);
        await ms.WriteAsync(header, CancellationToken.None);
        await ms.WriteAsync(payload, CancellationToken.None);
        ms.Position = 0;

        var ex = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            _ = await FrameCodec.ReadFrameStrictAsync<object>(ms, static _ => new object(), CancellationToken.None));

        Assert.Contains("CRC mismatch", ex.Message, StringComparison.Ordinal);
        Assert.Contains(wrongCrcHex, ex.Message, StringComparison.Ordinal);
        Assert.Contains(expectedCrcHex, ex.Message, StringComparison.Ordinal);
    }
}
