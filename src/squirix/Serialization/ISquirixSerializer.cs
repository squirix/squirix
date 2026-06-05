using System;
using System.IO;
using System.Text.Json;

namespace Squirix.Serialization;

/// <summary>
/// Abstraction over serialization used by Squirix components.
/// </summary>
public interface ISquirixSerializer
{
    /// <summary>
    /// Deserializes text into a value of <typeparamref name="T" />.
    /// </summary>
    /// <typeparam name="T">Target type.</typeparam>
    /// <param name="payload">Serialized payload.</param>
    /// <returns>Deserialized value.</returns>
    T? Deserialize<T>(string payload);

    /// <summary>
    /// Deserializes a JsonElement into <typeparamref name="T" />.
    /// </summary>
    /// <typeparam name="T">Target type.</typeparam>
    /// <param name="payload">JsonElement payload.</param>
    /// <returns>Deserialized value.</returns>
    T? Deserialize<T>(JsonElement payload);

    /// <summary>
    /// Deserializes UTF-8 data into <typeparamref name="T" />.
    /// </summary>
    /// <typeparam name="T">Target type.</typeparam>
    /// <param name="payload">Serialized payload (UTF-8).</param>
    /// <returns>Deserialized value.</returns>
    T? Deserialize<T>(ReadOnlySpan<byte> payload);

    /// <summary>
    /// Deserializes stream data into <typeparamref name="T" />.
    /// </summary>
    /// <typeparam name="T">Target type.</typeparam>
    /// <param name="payload">Stream providing serialized data.</param>
    /// <returns>Deserialized value.</returns>
    T? Deserialize<T>(Stream payload);

    /// <summary>
    /// Serializes a value into the provided destination stream.
    /// </summary>
    /// <typeparam name="T">Value type.</typeparam>
    /// <param name="destination">Stream that receives the serialized payload.</param>
    /// <param name="value">Value to serialize.</param>
    void Serialize<T>(Stream destination, T? value);

    /// <summary>
    /// Serializes a value into a JsonElement without allocating intermediate strings.
    /// </summary>
    /// <typeparam name="T">Value type.</typeparam>
    /// <param name="value">Value to serialize.</param>
    /// <returns>JsonElement representing the value.</returns>
    JsonElement SerializeToElement<T>(T? value);

    /// <summary>
    /// Serializes a value to a UTF-8 byte array.
    /// </summary>
    /// <typeparam name="T">Value type.</typeparam>
    /// <param name="value">Value to serialize.</param>
    /// <returns>Serialized payload.</returns>
    byte[] SerializeToUtf8Bytes<T>(T? value);
}
