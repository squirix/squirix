using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;

namespace Squirix.Server.Utils;

/// <summary>Exact-read helpers over <see cref="SafeFileHandle" /> with explicit offsets (no shared position state).</summary>
internal static class HandleEx
{
    /// <summary>Reads exactly <paramref name="buffer.Length" /> bytes at <paramref name="offset" />, advancing it.</summary>
    /// <param name="handle">The file handle to read from.</param>
    /// <param name="buffer">The span to fill completely.</param>
    /// <param name="offset">The file offset to start reading at; advanced by the number of bytes read.</param>
    /// <returns><see langword="false" /> when the file ends before the buffer is filled.</returns>
    internal static bool TryReadExact(SafeFileHandle handle, Span<byte> buffer, ref long offset)
    {
        var total = 0;
        while (total < buffer.Length)
        {
            var read = RandomAccess.Read(handle, buffer[total..], offset + total);
            if (read == 0)
                return false;

            total += read;
        }

        offset += buffer.Length;
        return true;
    }

    /// <summary>Asynchronously reads exactly <paramref name="buffer.Length" /> bytes at <paramref name="offset" />.</summary>
    /// <param name="handle">The file handle to read from.</param>
    /// <param name="buffer">The memory to fill completely.</param>
    /// <param name="offset">The file offset to start reading at.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The end offset on success; <see langword="null" /> when the file ends before the buffer is filled.</returns>
    internal static async ValueTask<long?> TryReadExactAsync(SafeFileHandle handle, Memory<byte> buffer, long offset, CancellationToken cancellationToken)
    {
        var total = 0;
        while (total < buffer.Length)
        {
            var read = await RandomAccess.ReadAsync(handle, buffer[total..], offset + total, cancellationToken).ConfigureAwait(false);
            if (read == 0)
                return null;

            total += read;
        }

        return offset + buffer.Length;
    }
}
