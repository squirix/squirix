using System;
using System.IO;
using System.Text.Json;
using Squirix.Serialization;

namespace Squirix.UnitTests.Serialization;

internal sealed class MarkerSerializer : ISquirixSerializer
{
    public T Deserialize<T>(string payload) => throw CreateNotUsed();

    public T Deserialize<T>(JsonElement payload) => throw CreateNotUsed();

    public T Deserialize<T>(ReadOnlySpan<byte> payload) => throw CreateNotUsed();

    public T Deserialize<T>(Stream payload) => throw CreateNotUsed();

    public void Serialize<T>(Stream destination, T? value) => throw CreateNotUsed();

    public JsonElement SerializeToElement<T>(T? value) => throw CreateNotUsed();

    public byte[] SerializeToUtf8Bytes<T>(T? value) => throw CreateNotUsed();

    private static NotSupportedException CreateNotUsed() => new("MarkerSerializer is an identity placeholder and must not be invoked.");
}
