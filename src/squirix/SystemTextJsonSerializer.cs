using System;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Squirix.Attributes;

namespace Squirix;

/// <summary><see cref="ISquirixSerializer" /> implementation backed by <see cref="System.Text.Json" />.</summary>
/// <remarks>
/// AOT-safe: every operation uses caller-provided metadata when supplied, otherwise metadata
/// from the <see cref="SerializerMetadata" /> chain is used without reflection. Types without
/// registered metadata throw <see cref="InvalidOperationException" /> instead of falling back to
/// runtime code generation, so trimming never silently breaks serialization. Cache value types
/// must be registered by the application (typically via a source-generated
/// <see cref="JsonSerializerContext" />).
/// </remarks>
[Immutable]
internal sealed class SystemTextJsonSerializer : ISquirixSerializer
{
    /// <inheritdoc />
    public T? Deserialize<T>(string payload, JsonTypeInfo<T>? typeInfo = null) => JsonSerializer.Deserialize(payload, typeInfo ?? SerializerMetadata.Resolve<T>());

    /// <inheritdoc />
    public T? Deserialize<T>(JsonElement payload, JsonTypeInfo<T>? typeInfo = null) => payload.ValueKind == JsonValueKind.Undefined || payload.ValueKind == JsonValueKind.Null
        ? default : payload.Deserialize(typeInfo ?? SerializerMetadata.Resolve<T>());

    /// <inheritdoc />
    public T? Deserialize<T>(ReadOnlySpan<byte> payload, JsonTypeInfo<T>? typeInfo = null) => JsonSerializer.Deserialize(payload, typeInfo ?? SerializerMetadata.Resolve<T>());

    /// <inheritdoc />
    public T? Deserialize<T>(Stream payload, JsonTypeInfo<T>? typeInfo = null) => JsonSerializer.Deserialize(payload, typeInfo ?? SerializerMetadata.Resolve<T>());

    /// <inheritdoc />
    public void Serialize<T>(Stream destination, T? value, JsonTypeInfo<T>? typeInfo = null)
    {
        if (typeInfo != null)
        {
            JsonSerializer.Serialize(destination, value!, typeInfo);
            return;
        }

        SerializerMetadata.Serialize(destination, value, typeof(T));
    }

    /// <inheritdoc />
    public JsonElement SerializeToElement<T>(T? value, JsonTypeInfo<T>? typeInfo = null)
    {
        return typeInfo != null
            ? JsonSerializer.SerializeToElement(value!, typeInfo)
            : SerializerMetadata.SerializeToElement(value, typeof(T));
    }

    /// <inheritdoc />
    public byte[] SerializeToUtf8Bytes<T>(T? value, JsonTypeInfo<T>? typeInfo = null)
    {
        return typeInfo != null
            ? JsonSerializer.SerializeToUtf8Bytes(value!, typeInfo)
            : SerializerMetadata.SerializeToUtf8Bytes(value, typeof(T));
    }
}
