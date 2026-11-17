using System;
using System.Buffers.Binary;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Builds outcome fixtures, including invalid payload-bearing encodings the production writer cannot emit.</summary>
internal static class ReplicaOutcomeTestBytes
{
    /// <summary>Builds the envelope supplied to corruption and effect-validation tests.</summary>
    /// <param name="applied">The encoded flag.</param>
    /// <param name="previous">A nonempty value makes the outcome invalid in the new format.</param>
    /// <returns>The test-owned bytes.</returns>
    internal static byte[] Encode(bool applied, ReadOnlyMemory<byte> previous)
    {
        var bytes = new byte[5 + previous.Length];
        bytes[0] = Convert.ToByte(applied);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(1, sizeof(int)), previous.Length);
        previous.Span.CopyTo(bytes.AsSpan(5));
        return bytes;
    }
}
