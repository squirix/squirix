using System;
using System.Buffers;
using System.IO;
using System.Text.Json;
using System.Threading;
using Squirix.Server.Storage.Journaling.Json;
using Squirix.Server.Storage.JournalProto;

namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// Read-only verification of a single journal segment stream (file header + framed records until EOF).
/// Invoked from file-based durability tools (friend assemblies; see <c>InternalsVisibleTo</c> in AssemblyInfo).
/// </summary>
internal static class JournalSegmentFileVerifier
{
    /// <summary>
    /// Verifies the journal segment stream: valid header, each frame passes CRC and JSON decode, and no trailing bytes remain when the stream is seekable.
    /// </summary>
    /// <param name="stream">The stream positioned at the start of the segment file.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="frames">Receives the number of successfully decoded frames.</param>
    /// <param name="lastSequence">Receives the maximum <see cref="JournalEnvelope.Seq" /> observed.</param>
    /// <param name="error">Human-readable failure reason when the method returns <see langword="false" />.</param>
    /// <returns><see langword="true" /> when the segment is fully valid for the verified prefix.</returns>
    internal static bool TryVerify(Stream stream, CancellationToken cancellationToken, out int frames, out ulong lastSequence, out string? error)
    {
        frames = 0;
        lastSequence = 0;
        error = null;

        if (!JournalFraming.TryReadAndValidateHeader(stream))
        {
            error = "invalid or missing journal file header";
            return false;
        }

        var frameOffset = (long)JournalFraming.FileHeaderSize;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var read = JournalFrameReader.ReadNext(stream, frameOffset, out var rentedBuffer, out var payloadLength);
            if (read.Status == JournalFrameReadStatus.EndOfFile)
                break;

            if (read.Status != JournalFrameReadStatus.Success)
            {
                error = JournalFrameDiagnostics.DescribeReadFailure(read);
                return false;
            }

            try
            {
                ArgumentNullException.ThrowIfNull(rentedBuffer);
                var payloadSpan = rentedBuffer.AsSpan(0, payloadLength);
                JournalEnvelope envelope;
                try
                {
                    envelope = RecordCodec.Deserialize(payloadSpan);
                }
                catch (JsonException)
                {
                    error = "journal frame JSON is invalid";
                    return false;
                }

                frames++;
                if (envelope.Seq > lastSequence)
                    lastSequence = envelope.Seq;
                frameOffset = read.NextFrameOffset;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rentedBuffer!);
            }
        }

        return !stream.CanSeek || stream.Position == stream.Length;
    }
}
