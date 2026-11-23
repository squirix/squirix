using Squirix.Transport.Wire.Codec;

namespace Squirix.Server.Core;

internal sealed class StoredText
{
    internal StoredText(byte[] owned)
    {
        Bytes = owned;
    }

    internal byte[] Bytes { get; }

    internal static T? Unpack<T>(T? value)
    {
        if (value is not StoredText text)
            return value;
        var decoded = text.Decode();
        return decoded is T typed ? typed : value;
    }

    internal string Decode() => WireReader.Utf8.GetString(Bytes);
}
