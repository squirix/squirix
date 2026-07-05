using System;
using System.IO;
using System.Text.Json;
using Squirix.Serialization;

namespace Squirix.UnitTests.Serialization;

internal sealed class SpySerializer : ISquirixSerializer
{
    private readonly ISquirixSerializer _inner;

    public SpySerializer(ISquirixSerializer inner)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    public int SerializeToUtf8BytesCalls { get; private set; }

    public int SerializeStreamCalls { get; private set; }

    public int DeserializeStringCalls { get; private set; }

    public int DeserializeStreamCalls { get; private set; }

    public T? Deserialize<T>(string payload)
    {
        DeserializeStringCalls++;
        return _inner.Deserialize<T>(payload);
    }

    public T? Deserialize<T>(JsonElement payload) => _inner.Deserialize<T>(payload);

    public T? Deserialize<T>(ReadOnlySpan<byte> payload) => _inner.Deserialize<T>(payload);

    public T? Deserialize<T>(Stream payload)
    {
        DeserializeStreamCalls++;
        return _inner.Deserialize<T>(payload);
    }

    public void Serialize<T>(Stream destination, T? value)
    {
        SerializeStreamCalls++;
        _inner.Serialize(destination, value);
    }

    public JsonElement SerializeToElement<T>(T? value) => _inner.SerializeToElement(value);

    public byte[] SerializeToUtf8Bytes<T>(T? value)
    {
        SerializeToUtf8BytesCalls++;
        return _inner.SerializeToUtf8Bytes(value);
    }
}
