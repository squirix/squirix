using System;
using System.IO;
using System.Text.Json;
using Squirix.Serialization;

namespace Squirix.UnitTests.Serialization;

internal sealed class StreamSerializeFaultSerializer : ISquirixSerializer
{
    public T Deserialize<T>(string payload) => throw CreateNotUsed();

    public T Deserialize<T>(JsonElement payload) => throw CreateNotUsed();

    public T Deserialize<T>(ReadOnlySpan<byte> payload) => throw CreateNotUsed();

    public T Deserialize<T>(Stream payload) => throw CreateNotUsed();

    public void Serialize<T>(Stream destination, T? value) => throw new InvalidOperationException("fail");

    public JsonElement SerializeToElement<T>(T? value) => throw CreateNotUsed();

    public byte[] SerializeToUtf8Bytes<T>(T? value) => throw CreateNotUsed();

    private static NotSupportedException CreateNotUsed() => new("StreamSerializeFaultSerializer only implements stream serialize failure.");
}
