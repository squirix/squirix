using System;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Squirix.Server.Attributes;

namespace Squirix.Server.Core;

/// <summary><see cref="IServerSerializer" /> implementation backed by <see cref="System.Text.Json" />.</summary>
/// <remarks>
/// AOT-safe: every operation uses caller-provided metadata when supplied, otherwise metadata
/// registered for the value type is resolved without reflection from source-generated contexts.
/// Types without registered metadata throw <see cref="InvalidOperationException" /> instead of
/// falling back to runtime code generation, so trimming never silently breaks serialization.
/// </remarks>
[Immutable]
internal sealed class ServerJsonSerializer : IServerSerializer
{
    /// <summary>Initializes a new instance of the <see cref="ServerJsonSerializer" /> class.</summary>
    internal ServerJsonSerializer()
    {
    }

    /// <inheritdoc />
    public T? Deserialize<T>(string payload, JsonTypeInfo<T>? typeInfo = null) => JsonSerializer.Deserialize(payload, typeInfo ?? ServerSerializerMetadata.Resolve<T>());

    /// <inheritdoc />
    public T? Deserialize<T>(ReadOnlySpan<byte> payload, JsonTypeInfo<T>? typeInfo = null) => JsonSerializer.Deserialize(payload, typeInfo ?? ServerSerializerMetadata.Resolve<T>());

    /// <inheritdoc />
    public JsonElement SerializeToElement<T>(T? value, JsonTypeInfo<T>? typeInfo = null)
    {
        return typeInfo != null
            ? JsonSerializer.SerializeToElement(value!, typeInfo)
            : ServerSerializerMetadata.SerializeToElement(value, typeof(T));
    }
}
