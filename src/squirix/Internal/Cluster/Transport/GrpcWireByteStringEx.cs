using Google.Protobuf;

namespace Squirix.Internal.Cluster.Transport;

/// <summary>Zero-copy protobuf payload wrappers for gRPC wire encoding.</summary>
internal static class GrpcWireByteStringEx
{
    internal static ByteString WrapPayload(byte[] payload) =>
        payload.Length is 0 ? ByteString.Empty : UnsafeByteOperations.UnsafeWrap(payload);
}
