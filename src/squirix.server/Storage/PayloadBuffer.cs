using System;
using System.Buffers;

namespace Squirix.Server.Storage;

/// <summary>Keeps oversized journal and snapshot buffers out of the shared power-of-two buckets.</summary>
internal static class PayloadBuffer
{
    private const int LargestPooledBytes = 32 * 1024 * 1024;

    internal static byte[] Rent(int length) => length > LargestPooledBytes
        ? GC.AllocateUninitializedArray<byte>(length)
        : ArrayPool<byte>.Shared.Rent(length);

    internal static void Release(byte[] buffer)
    {
        if (buffer.Length <= LargestPooledBytes)
            ArrayPool<byte>.Shared.ReturnCleared(buffer);
    }
}
