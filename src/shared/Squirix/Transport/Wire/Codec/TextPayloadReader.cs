using System.IO;
using System.Text.Unicode;

namespace Squirix.Transport.Wire.Codec;

internal static class TextPayloadReader
{
    internal static byte[] Read(ref WireReader reader, bool tail)
    {
        var bytes = tail ? reader.TailBytes(out var owned) : reader.Bytes(out owned);
        return Utf8.IsValid(bytes.Span)
            ? owned ?? WireReader.CopyBytes(bytes.Span)
            : throw new InvalidDataException("Invalid UTF-8 in a wire string.");
    }
}
