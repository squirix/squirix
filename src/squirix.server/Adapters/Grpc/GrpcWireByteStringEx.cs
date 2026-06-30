using System;
using System.Runtime.InteropServices;
using Google.Protobuf;

namespace Squirix.Server.Adapters.Grpc;

/// <summary>Zero-copy protobuf payload wrappers for gRPC wire encoding.</summary>
internal static class GrpcWireByteStringEx
{
    internal static ByteString WrapPayload(byte[] payload) =>
        payload.Length is 0 ? ByteString.Empty : UnsafeByteOperations.UnsafeWrap(payload);

    internal static ByteString WrapPayload(ReadOnlyMemory<byte> payload)
    {
        if (payload.IsEmpty)
            return ByteString.Empty;

        if (MemoryMarshal.TryGetArray(payload, out var segment) && segment.Array is not null && segment.Offset is 0 && segment.Count == segment.Array.Length)
            return UnsafeByteOperations.UnsafeWrap(segment.Array);

        return ByteString.CopyFrom(payload.Span);
    }
}
