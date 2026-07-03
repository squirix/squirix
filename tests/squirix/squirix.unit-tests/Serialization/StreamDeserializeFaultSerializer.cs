using System;
using System.IO;
using System.Text.Json;
using Squirix.Serialization;

namespace Squirix.UnitTests.Serialization;

internal sealed class StreamDeserializeFaultSerializer : ISquirixSerializer
{
    private readonly ISquirixSerializer _inner;

    public StreamDeserializeFaultSerializer(ISquirixSerializer inner)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    public T? Deserialize<T>(string payload) => _inner.Deserialize<T>(payload);

    public T? Deserialize<T>(JsonElement payload) => _inner.Deserialize<T>(payload);

    public T? Deserialize<T>(ReadOnlySpan<byte> payload) => _inner.Deserialize<T>(payload);

    public T Deserialize<T>(Stream payload) => throw new InvalidOperationException("boom");

    public void Serialize<T>(Stream destination, T? value) => _inner.Serialize(destination, value);

    public JsonElement SerializeToElement<T>(T? value) => _inner.SerializeToElement(value);

    public byte[] SerializeToUtf8Bytes<T>(T? value) => _inner.SerializeToUtf8Bytes(value);
}
