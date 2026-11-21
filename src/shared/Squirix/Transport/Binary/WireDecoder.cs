namespace Squirix.Transport.Binary;

internal delegate T WireDecoder<out T>(ref WireReader reader);
