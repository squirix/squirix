using System;
using System.IO;

namespace Squirix.Server.Utils;

internal static class StreamEx
{
    internal static bool TryReadExact(Stream stream, Span<byte> buffer)
    {
        while (!buffer.IsEmpty)
        {
            var read = stream.Read(buffer);
            if (read == 0)
                return false;

            buffer = buffer[read..];
        }

        return true;
    }
}
