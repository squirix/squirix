namespace Squirix.Transport.Wire.Codec;

/// <summary>A binary message or nested value with the same encoding for transport and fingerprints.</summary>
internal interface IWireBody
{
    void Encode(ref WireWriter writer);
}
