using System.Text;
using Squirix.Transport.Wire.Codec;

namespace Squirix.Server.Core;

internal sealed class StoredText
{
    internal StoredText(byte[] owned)
    {
        Bytes = owned;
    }

    internal byte[] Bytes { get; }

    internal static T? Pack<T>(T? value)
    {
        if (typeof(T) != typeof(object) || value is not string text)
            return value;
        try
        {
            var stored = new StoredText(WireReader.Utf8.GetBytes(text));
            return System.Runtime.CompilerServices.Unsafe.As<StoredText, T>(ref stored);
        }
        catch (EncoderFallbackException)
        {
            return value;
        }
    }

    internal static T? Unpack<T>(T? value)
    {
        if (value is not StoredText text)
            return value;
        var decoded = text.Decode();
        return decoded is T typed ? typed : value;
    }

    internal string Decode() => WireReader.Utf8.GetString(Bytes);
}
