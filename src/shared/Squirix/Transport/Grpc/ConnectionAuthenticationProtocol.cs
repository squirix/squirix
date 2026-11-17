using System;
using System.Buffers.Binary;

namespace Squirix.Transport.Grpc;

/// <summary>Defines the bounded authentication exchange preceding HTTP on a TLS connection.</summary>
internal static class ConnectionAuthenticationProtocol
{
    internal const int HeaderLength = 8;
    internal const int MaximumTokenBytes = 8192;
    internal const byte Accepted = 0;
    internal const byte InvalidCredentials = 1;
    internal const byte Busy = 2;
    internal const byte UnsupportedVersion = 3;
    internal const string RenewalRequired = "connection-authentication-renewal-required";

    private const byte Version = 1;

    internal static bool HasRequestMagic(ReadOnlySpan<byte> bytes) => bytes.Length >= 4 && bytes[..4].SequenceEqual("SQAU"u8);

    internal static void WriteRequestHeader(Span<byte> header, int tokenBytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(tokenBytes);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(tokenBytes, MaximumTokenBytes);
        "SQAU"u8.CopyTo(header);
        header[4] = Version;
        header[5] = 0;
        BinaryPrimitives.WriteUInt16BigEndian(header[6..], ushort.CreateChecked(tokenBytes));
    }

    internal static byte ReadRequestHeader(ReadOnlySpan<byte> header, out int tokenBytes)
    {
        tokenBytes = 0;
        if (header.Length != HeaderLength || !HasRequestMagic(header))
            return InvalidCredentials;
        if (header[4] != Version)
            return UnsupportedVersion;
        if (header[5] != 0)
            return InvalidCredentials;
        tokenBytes = BinaryPrimitives.ReadUInt16BigEndian(header[6..]);
        return tokenBytes is > 0 and <= MaximumTokenBytes ? Accepted : InvalidCredentials;
    }

    internal static void WriteAcknowledgement(Span<byte> header, byte status)
    {
        "SQAA"u8.CopyTo(header);
        header[4] = Version;
        header[5] = status;
        header[6] = 0;
        header[7] = 0;
    }

    internal static bool TryReadAcknowledgement(ReadOnlySpan<byte> header, out byte status)
    {
        status = InvalidCredentials;
        if (header.Length != HeaderLength || !header[..4].SequenceEqual("SQAA"u8) || header[4] != Version || header[6] != 0 || header[7] != 0)
            return false;
        status = header[5];
        return status <= UnsupportedVersion;
    }
}
