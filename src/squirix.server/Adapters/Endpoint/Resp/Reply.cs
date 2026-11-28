using System;
using System.Buffers;
using System.Buffers.Text;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Adapters.Endpoint.Resp;

internal static class Reply
{
    private static readonly byte[] NullBulk = [.. "$-1\r\n"u8];
    private static readonly byte[] Null = [.. "_\r\n"u8];
    private static readonly byte[] Terminator = [.. "\r\n"u8];

    internal static ValueTask WriteNullAsync(Stream stream, bool resp3, CancellationToken cancellationToken) =>
        stream.WriteAsync(resp3 ? Null : NullBulk, cancellationToken);

    internal static ValueTask WriteIntegerAsync(Stream stream, long value, CancellationToken cancellationToken) =>
        stream.WriteAsync(FormatNumber(":"u8[0], value), cancellationToken);

    internal static async ValueTask WriteBulkAsync(Stream stream, ReadOnlyMemory<byte> value, CancellationToken cancellationToken)
    {
        var header = FormatNumber("$"u8[0], value.Length);
        var length = checked(header.Length + value.Length + Terminator.Length);
        var buffer = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            header.CopyTo(buffer.AsMemory());
            value.CopyTo(buffer.AsMemory(header.Length));
            Terminator.CopyTo(buffer.AsMemory(header.Length + value.Length));
            await stream.WriteAsync(buffer.AsMemory(0, length), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static ReadOnlyMemory<byte> FormatNumber(byte prefix, long value)
    {
#pragma warning disable ZA0301 // The header outlives this method during an asynchronous stream write.
        var buffer = new byte[23];
#pragma warning restore ZA0301
        buffer[0] = prefix;
        _ = Utf8Formatter.TryFormat(value, buffer.AsSpan(1, 20), out var written);
        buffer[written + 1] = Terminator[0];
        buffer[written + 2] = Terminator[1];
        return buffer.AsMemory(0, written + 3);
    }
}
