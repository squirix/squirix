namespace Squirix.Transport.Wire.Codec;

internal delegate T WireDecoder<out T>(ref WireReader reader);
