using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;

namespace Squirix.Transport.Binary;

/// <summary>Encodes the trace identifier, span identifier and flags of a version-zero W3C traceparent.</summary>
internal static class WireTraceParent
{
    internal const int ByteCount = 25;

    private static readonly SearchValues<char> Characters = SearchValues.Create("0123456789abcdef-");

    internal static void Write(Span<byte> destination, in ActivityContext context)
    {
        context.TraceId.CopyTo(destination[..16]);
        context.SpanId.CopyTo(destination.Slice(16, 8));
        destination[24] = byte.CreateTruncating(Unsafe.BitCast<ActivityTraceFlags, int>(context.TraceFlags));
    }

    internal static bool TryWrite(ReadOnlySpan<char> text, Span<byte> destination) =>
        text.Length == 55 && text.StartsWith("00-", StringComparison.Ordinal) && text[35] == '-' && text[52] == '-' && !text.ContainsAnyExcept(Characters) &&
        Convert.FromHexString(text.Slice(3, 32), destination[..16], out _, out _) == OperationStatus.Done &&
        Convert.FromHexString(text.Slice(36, 16), destination.Slice(16, 8), out _, out _) == OperationStatus.Done &&
        Convert.FromHexString(text[53..], destination.Slice(24, 1), out _, out _) == OperationStatus.Done;

    internal static string Read(ReadOnlySpan<byte> bytes) => bytes.Length == ByteCount ? Format(bytes) : throw new InvalidDataException("Invalid W3C traceparent length.");

    private static string Format(ReadOnlySpan<byte> bytes) => string.Create(55, bytes, static (result, source) =>
        {
            "00-".CopyTo(result);
            _ = Convert.TryToHexStringLower(source[..16], result[3..35], out _);
            result[35] = '-';
            _ = Convert.TryToHexStringLower(source.Slice(16, 8), result[36..52], out _);
            result[52] = '-';
            _ = Convert.TryToHexStringLower(source[24..], result[53..], out _);
        });
}
