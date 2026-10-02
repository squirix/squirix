using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Security.Cryptography;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Adapters.Grpc;

/// <summary>Builds deterministic fingerprints for mutating cache RPC requests.</summary>
/// <remarks>
/// A fingerprint is the uppercase hex SHA-256 of the operation, cache name and key, each as its 32-bit little-endian UTF-16 length
/// followed by its UTF-16 code units, then the SHA-256 of the request message when there is one. The lengths make the encoding
/// unambiguous, so requests that only differ in where one field ends and the next begins never share a fingerprint.
/// </remarks>
internal static class RpcMutationFingerprints
{
    private const int DigestBytes = 32;
    private const int StackBufferBytes = 1024;

    internal static string AddEntryIfAbsent(string cacheName, string key, CacheEntryWire entry) => Fingerprint("try-add-entry-async", cacheName, key, entry);

    internal static string GetOrAdd(string cacheName, string key, CacheEntryWire entry) => Fingerprint("get-or-add-async", cacheName, key, entry);

    internal static string Remove(string cacheName, string key) => Fingerprint("remove-async", cacheName, key, null);

    internal static string RemoveExpiration(string cacheName, string key) => Fingerprint("remove-expiration-async", cacheName, key, null);

    internal static string SetEntry(string cacheName, string key, CacheEntryWire entry) => Fingerprint("set-entry-async", cacheName, key, entry);

    internal static string Touch(string cacheName, string key, Duration expiration) => Fingerprint("touch-async", cacheName, key, expiration);

    internal static string Update(string cacheName, string key, CacheEntryWire entry) => Fingerprint("update-async", cacheName, key, entry);

    private static string Fingerprint(string operation, string cacheName, string key, IMessage? message)
    {
        var length = checked(FieldBytes(operation) + FieldBytes(cacheName) + FieldBytes(key) + DigestBytes);
        Span<byte> fingerprint = stackalloc byte[DigestBytes];
        if (length <= StackBufferBytes)
        {
            Span<byte> buffer = stackalloc byte[length];
            HashFields(buffer, operation, cacheName, key, message, fingerprint);
        }
        else
        {
            var rented = ArrayPool<byte>.Shared.Rent(length);
            try
            {
                HashFields(rented.AsSpan(0, length), operation, cacheName, key, message, fingerprint);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        Span<char> hex = stackalloc char[DigestBytes * 2];
        HexFormat.WriteSha256HexUpper(hex, fingerprint);
        return new string(hex);
    }

    private static int FieldBytes(string value) => checked(sizeof(int) + (value.Length * sizeof(char)));

    private static void HashFields(Span<byte> buffer, string operation, string cacheName, string key, IMessage? message, Span<byte> fingerprint)
    {
        var written = WriteField(buffer, operation);
        written += WriteField(buffer[written..], cacheName);
        written += WriteField(buffer[written..], key);
        if (message != null)
        {
            HashMessage(message, buffer.Slice(written, DigestBytes));
            written += DigestBytes;
        }

        _ = SHA256.HashData(buffer[..written], fingerprint);
    }

    private static void HashMessage(IMessage message, Span<byte> digest)
    {
        var size = message.CalculateSize();
        if (size <= StackBufferBytes / 2)
        {
            Span<byte> buffer = stackalloc byte[size];
            message.WriteTo(buffer);
            _ = SHA256.HashData(buffer, digest);
            return;
        }

        var rented = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            message.WriteTo(rented.AsSpan(0, size));
            _ = SHA256.HashData(rented.AsSpan(0, size), digest);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static int WriteField(Span<byte> destination, string value)
    {
        BinaryPrimitives.WriteInt32LittleEndian(destination, value.Length);
        var chars = destination.Slice(sizeof(int), value.Length * sizeof(char));
        for (var i = 0; i < value.Length; i++)
            BinaryPrimitives.WriteUInt16LittleEndian(chars[(i * sizeof(char))..], value[i]);

        return sizeof(int) + chars.Length;
    }
}
