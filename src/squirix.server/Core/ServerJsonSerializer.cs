using System;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Squirix.Server.Core;

/// <summary>Server JSON serialization backed by <see cref="System.Text.Json" />.</summary>
/// <remarks>
/// AOT-safe: every operation uses caller-provided metadata when supplied, otherwise metadata
/// registered for the value type is resolved without reflection from source-generated contexts.
/// Types without registered metadata throw <see cref="InvalidOperationException" /> instead of
/// falling back to runtime code generation, so trimming never silently breaks serialization.
/// </remarks>
internal static class ServerJsonSerializer
{
    /// <summary>Deserializes text into a value of <typeparamref name="T" />.</summary>
    /// <typeparam name="T">Target type.</typeparam>
    /// <param name="payload">Serialized payload.</param>
    /// <param name="typeInfo">AOT-safe metadata for <typeparamref name="T" />; resolved from registered contexts when <see langword="null" />.</param>
    /// <returns>Deserialized value.</returns>
    internal static T? Deserialize<T>(string payload, JsonTypeInfo<T>? typeInfo = null) => JsonSerializer.Deserialize(payload, typeInfo ?? ServerSerializerMetadata.Resolve<T>());

    /// <summary>Deserializes UTF-8 data into <typeparamref name="T" />.</summary>
    /// <typeparam name="T">Target type.</typeparam>
    /// <param name="payload">Serialized payload (UTF-8).</param>
    /// <param name="typeInfo">AOT-safe metadata for <typeparamref name="T" />; resolved from registered contexts when <see langword="null" />.</param>
    /// <returns>Deserialized value.</returns>
    internal static T? Deserialize<T>(ReadOnlySpan<byte> payload, JsonTypeInfo<T>? typeInfo = null) => JsonSerializer.Deserialize(payload, typeInfo ?? ServerSerializerMetadata.Resolve<T>());

    /// <summary>Serializes a value into a JsonElement without allocating intermediate strings.</summary>
    /// <typeparam name="T">Value type.</typeparam>
    /// <param name="value">Value to serialize.</param>
    /// <param name="typeInfo">AOT-safe metadata for <typeparamref name="T" />; resolved from registered contexts when <see langword="null" />.</param>
    /// <returns>JsonElement representing the value.</returns>
    internal static JsonElement SerializeToElement<T>(T? value, JsonTypeInfo<T>? typeInfo = null)
    {
        return typeInfo != null
            ? JsonSerializer.SerializeToElement(value!, typeInfo)
            : ServerSerializerMetadata.SerializeToElement(value, typeof(T));
    }
}
