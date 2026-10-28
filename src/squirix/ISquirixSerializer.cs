using System;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Squirix;

/// <summary>Abstraction over serialization used by Squirix components.</summary>
/// <remarks>
/// Cache values that are not a scalar, a <see cref="JsonElement" /> or a <see cref="decimal" /> travel as the bytes of
/// <see cref="SerializeToUtf8Bytes{T}" /> and are read back through <see cref="Deserialize{T}(ReadOnlySpan{byte}, JsonTypeInfo{T})" />.
/// The server refuses bytes that are not exactly one well-formed JSON value (UTF-8, no byte order mark, no comments, no trailing
/// commas, nesting of at most 64 levels), so an implementation must write JSON, and it must not change a returned array afterwards.
/// All members are AOT-safe when <c language="csharp">typeInfo</c> is provided.
/// When it is omitted, metadata is resolved from registered source-generation contexts;
/// types without registered metadata throw <see cref="InvalidOperationException" /> instead
/// of falling back to reflection, so trimming never silently breaks serialization.
/// </remarks>
public interface ISquirixSerializer
{
    /// <summary>Deserializes text into a value of <typeparamref name="T" />.</summary>
    /// <typeparam name="T">Target type.</typeparam>
    /// <param name="payload">Serialized payload.</param>
    /// <param name="typeInfo">AOT-safe metadata for <typeparamref name="T" />; resolved from registered contexts when <see langword="null" />.</param>
    /// <returns>Deserialized value.</returns>
    T? Deserialize<T>(string payload, JsonTypeInfo<T>? typeInfo = null);

    /// <summary>Deserializes a JsonElement into <typeparamref name="T" />.</summary>
    /// <typeparam name="T">Target type.</typeparam>
    /// <param name="payload">JsonElement payload.</param>
    /// <param name="typeInfo">AOT-safe metadata for <typeparamref name="T" />; resolved from registered contexts when <see langword="null" />.</param>
    /// <returns>Deserialized value.</returns>
    T? Deserialize<T>(JsonElement payload, JsonTypeInfo<T>? typeInfo = null);

    /// <summary>Deserializes UTF-8 data into <typeparamref name="T" />.</summary>
    /// <typeparam name="T">Target type.</typeparam>
    /// <param name="payload">Serialized payload (UTF-8).</param>
    /// <param name="typeInfo">AOT-safe metadata for <typeparamref name="T" />; resolved from registered contexts when <see langword="null" />.</param>
    /// <returns>Deserialized value.</returns>
    T? Deserialize<T>(ReadOnlySpan<byte> payload, JsonTypeInfo<T>? typeInfo = null);

    /// <summary>Deserializes stream data into <typeparamref name="T" />.</summary>
    /// <typeparam name="T">Target type.</typeparam>
    /// <param name="payload">Stream providing serialized data.</param>
    /// <param name="typeInfo">AOT-safe metadata for <typeparamref name="T" />; resolved from registered contexts when <see langword="null" />.</param>
    /// <returns>Deserialized value.</returns>
    T? Deserialize<T>(Stream payload, JsonTypeInfo<T>? typeInfo = null);

    /// <summary>Serializes a value into the provided destination stream.</summary>
    /// <typeparam name="T">Value type.</typeparam>
    /// <param name="destination">Stream that receives the serialized payload.</param>
    /// <param name="value">Value to serialize.</param>
    /// <param name="typeInfo">AOT-safe metadata for <typeparamref name="T" />; resolved from registered contexts when <see langword="null" />.</param>
    void Serialize<T>(Stream destination, T? value, JsonTypeInfo<T>? typeInfo = null);

    /// <summary>Serializes a value into a JsonElement without allocating intermediate strings.</summary>
    /// <typeparam name="T">Value type.</typeparam>
    /// <param name="value">Value to serialize.</param>
    /// <param name="typeInfo">AOT-safe metadata for <typeparamref name="T" />; resolved from registered contexts when <see langword="null" />.</param>
    /// <returns>JsonElement representing the value.</returns>
    JsonElement SerializeToElement<T>(T? value, JsonTypeInfo<T>? typeInfo = null);

    /// <summary>Serializes a value to a UTF-8 byte array holding exactly one JSON value.</summary>
    /// <typeparam name="T">Value type.</typeparam>
    /// <param name="value">Value to serialize.</param>
    /// <param name="typeInfo">AOT-safe metadata for <typeparamref name="T" />; resolved from registered contexts when <see langword="null" />.</param>
    /// <returns>Serialized payload; the client sends the returned array without copying it, so the caller must not modify it afterwards.</returns>
    byte[] SerializeToUtf8Bytes<T>(T? value, JsonTypeInfo<T>? typeInfo = null);
}
