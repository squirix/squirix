namespace Squirix.Transport.Wire.Codec;

internal static class TextPayloadReader
{
    internal static string Read(ref WireReader reader, bool tail) => reader.Text(tail);
}
