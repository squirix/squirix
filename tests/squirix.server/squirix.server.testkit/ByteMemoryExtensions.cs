using System;

namespace Squirix.Server.TestKit;

/// <summary>Test helpers for building byte memory from literals.</summary>
public static class ByteMemoryExtensions
{
    /// <param name="memory">The memory type the helpers build.</param>
    extension(ReadOnlyMemory<byte> memory)
    {
        /// <summary>Copies <paramref name="bytes" /> into a memory that outlives the call.</summary>
        /// <param name="bytes">The bytes in order.</param>
        /// <returns>A memory holding the bytes.</returns>
        public static ReadOnlyMemory<byte> Of(params ReadOnlySpan<byte> bytes)
        {
            var copy = new byte[bytes.Length];
            bytes.CopyTo(copy);
            return copy;
        }
    }
}
