using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage;

/// <summary>
/// Framed I/O for journal/snapshots: [len:4][crc32c:4][payload...].
/// Uses software CRC-32C (Castagnoli) — no external packages required.
/// </summary>
internal static class FrameCodec
{
    /// <summary>
    /// Reads a single frame and converts the validated payload while the backing buffer is owned by this method.
    /// </summary>
    /// <param name="s">The source stream to read from.</param>
    /// <param name="payloadReader">A synchronous reader invoked only when the frame payload is complete and the CRC matches.</param>
    /// <param name="cancellationToken">A token to observe while the read operations are in progress.</param>
    /// <typeparam name="T">The result type produced from the payload.</typeparam>
    /// <returns>
    /// <c>Ok = true</c> with the converted result when a full, valid frame was read; otherwise, <c>Ok = false</c>.
    /// </returns>
    /// <remarks>
    /// The payload memory is invalid after this method returns because it is backed by a pooled buffer.
    /// </remarks>
    public static async ValueTask<(bool Ok, T? Result)> ReadFrameAsync<T>(Stream s, Func<ReadOnlyMemory<byte>, T> payloadReader, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(payloadReader);

        var header = new byte[8];
        var r = await ReadExactlyOrDefaultAsync(s, header, cancellationToken).ConfigureAwait(false);
        if (r is 0 or < 8)
            return (false, default);

        var len = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(0, 4));
        var exp = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(4, 4));

        // Sanity cap to avoid absurd rentals when a file is corrupted
        const uint maxFrameBytes = 1u << 30; // 1 GiB
        if (len > maxFrameBytes)
            return (false, default);

        var rented = ArrayPool<byte>.Shared.Rent((int)len);
        var payload = rented.AsMemory(0, (int)len);
        try
        {
            r = await ReadExactlyOrDefaultAsync(s, payload, cancellationToken).ConfigureAwait(false);
            if (r < len)
                return (false, default); // torn payload

            var got = Crc32C.Compute(payload.Span);
            return got == exp ? (true, payloadReader(payload)) : (false, default);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    public static async ValueTask<(bool Ok, T? Result)> ReadFrameStrictAsync<T>(Stream s, Func<ReadOnlyMemory<byte>, T> payloadReader, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(payloadReader);

        var header = new byte[8];
        var r = await ReadExactlyOrDefaultAsync(s, header, cancellationToken).ConfigureAwait(false);
        if (r == 0)
            return (false, default);

        if (r < header.Length)
            throw new InvalidDataException("Snapshot frame header is truncated.");

        var len = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(0, 4));
        var exp = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(4, 4));

        const uint maxFrameBytes = 1u << 30; // 1 GiB
        if (len > maxFrameBytes)
            throw new InvalidDataException("Snapshot frame length exceeds the supported maximum.");

        var rented = ArrayPool<byte>.Shared.Rent((int)len);
        var payload = rented.AsMemory(0, (int)len);
        try
        {
            r = await ReadExactlyOrDefaultAsync(s, payload, cancellationToken).ConfigureAwait(false);
            if (r < len)
                throw new InvalidDataException("Snapshot frame payload is truncated.");

            var got = Crc32C.Compute(payload.Span);
            var message = $"Snapshot frame CRC mismatch (expected {HexFormat.FormatUInt32HexLower(exp)}, actual {HexFormat.FormatUInt32HexLower(got)}).";
            return got != exp ? throw new InvalidDataException(message) : (true, payloadReader(payload));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Writes a single binary frame to the stream in the format <c>[len][crc32c][payload]</c>.
    /// </summary>
    /// <param name="s">
    /// The destination <see cref="Stream" />. Must be writable and support asynchronous writes.
    /// The stream is neither flushed nor closed by this method.
    /// </param>
    /// <param name="payload">
    /// The payload bytes to write. The 32-bit little-endian <c>len</c> field is the length of this payload,
    /// and the 32-bit little-endian <c>crc32c</c> field is computed over the payload using the Castagnoli polynomial.
    /// </param>
    /// <param name="cancellationToken">
    /// A token to observe while the write operations are in progress.
    /// </param>
    /// <returns>
    /// A <see cref="ValueTask" /> that completes when both the 8-byte header and the payload have been written.
    /// </returns>
    /// <remarks>
    ///     <para>
    ///     Frame layout (all fields little-endian):
    ///     <list type="number">
    ///         <item>
    ///             <description><c>len</c> — <see cref="uint" /> payload length (4 bytes)</description>
    ///         </item>
    ///         <item>
    ///             <description><c>crc32c</c> — CRC-32C of the payload (4 bytes)</description>
    ///         </item>
    ///         <item>
    ///             <description><c>payload</c> — raw payload bytes</description>
    ///         </item>
    ///     </list>
    ///     </para>
    ///     <para>
    ///     The method does not validate that <paramref name="payload" /> length fits in <see cref="uint" />; callers should ensure
    ///     it does (≤ 4,294,967,295) to avoid overflow/truncation in the length field.
    ///     </para>
    ///     <para>
    ///     The stream position advances by <c>8 + payload.Length</c>. Consider calling <see cref="Stream.FlushAsync(CancellationToken)" />
    ///     if durability is required.
    ///     </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown by underlying APIs if <paramref name="s" /> is <c>null</c>.</exception>
    /// <exception cref="ObjectDisposedException">The stream has been disposed.</exception>
    /// <exception cref="NotSupportedException">The stream does not support writing.</exception>
    /// <exception cref="IOException">An I/O error occurred during writing.</exception>
    /// <exception cref="OperationCanceledException">The operation was canceled via <paramref name="cancellationToken" />.</exception>
    public static async ValueTask WriteFrameAsync(Stream s, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        // Header = 8 bytes: len (LE) + crc32c (LE)
        var header = new byte[8];
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(0, 4), (uint)payload.Length);
        var crc = Crc32C.Compute(payload.Span);
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(4, 4), crc);

        await s.WriteAsync(header, cancellationToken).ConfigureAwait(false);
        await s.WriteAsync(payload, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads exactly 'buf.Length' bytes unless EOF occurs. Returns the number of bytes actually read.
    /// </summary>
    private static async Task<int> ReadExactlyOrDefaultAsync(Stream s, byte[] buf, CancellationToken cancellationToken) =>
        await ReadExactlyOrDefaultAsync(s, buf.AsMemory(), cancellationToken).ConfigureAwait(false);

    private static async Task<int> ReadExactlyOrDefaultAsync(Stream s, Memory<byte> buf, CancellationToken cancellationToken)
    {
        int off = 0, need = buf.Length;
        while (need > 0)
        {
            var n = await s.ReadAsync(buf.Slice(off, need), cancellationToken).ConfigureAwait(false);
            if (n == 0)
                break;

            off += n;
            need -= n;
        }

        return off;
    }
}
