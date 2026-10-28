using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Text.Json;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Core;

/// <summary>A structured cache value kept as the validated UTF-8 JSON bytes the client sent, never parsed or rewritten by the server.</summary>
/// <remarks>The bytes are immutable: the value shares them with the request that carried them and with every response that returns it.</remarks>
[Immutable]
internal sealed class JsonUtf8Value
{
    /// <summary>The bytes the value adds to the heap on top of its JSON text: the value object with its memory reference and the header of the array it points to.</summary>
    internal const int OverheadBytes = 56;

    private JsonUtf8Value(ReadOnlyMemory<byte> bytes)
    {
        Bytes = bytes;
    }

    /// <summary>Gets the JSON text of the value.</summary>
    internal ReadOnlyMemory<byte> Bytes { get; }

    /// <summary>Gets the length of the JSON text in bytes.</summary>
    internal int Length => Bytes.Length;

    /// <summary>Wraps bytes the caller owns and never changes, after checking that they are one JSON value.</summary>
    /// <param name="ownedBytes">The JSON text; the value keeps a reference to it without copying.</param>
    /// <param name="value">The value, or <see langword="null" /> when the bytes are not one JSON value.</param>
    /// <returns><see langword="true" /> when the bytes are valid.</returns>
    internal static bool TryCreate(ReadOnlyMemory<byte> ownedBytes, [NotNullWhen(true)] out JsonUtf8Value? value)
    {
        if (!JsonUtf8Validator.IsValid(ownedBytes.Span))
        {
            value = null;
            return false;
        }

        value = new JsonUtf8Value(ownedBytes);
        return true;
    }

    /// <summary>Copies bytes that may be overwritten later, such as a pooled buffer, after checking that they are one JSON value.</summary>
    /// <param name="bytes">The JSON text.</param>
    /// <param name="value">The value over a copy of the bytes, or <see langword="null" /> when the bytes are not one JSON value.</param>
    /// <returns><see langword="true" /> when the bytes are valid.</returns>
    internal static bool TryCreateCopy(ReadOnlySpan<byte> bytes, [NotNullWhen(true)] out JsonUtf8Value? value)
    {
        if (!JsonUtf8Validator.IsValid(bytes))
        {
            value = null;
            return false;
        }

        value = new JsonUtf8Value(BufferEx.CopyToOwned(bytes));
        return true;
    }

    /// <summary>Wraps serializer output, which is JSON by construction, still checking it so a faulty converter cannot reach storage.</summary>
    /// <param name="serialized">The serializer output; the value takes it over.</param>
    /// <returns>The value.</returns>
    /// <exception cref="InvalidOperationException">The bytes are not one JSON value.</exception>
    internal static JsonUtf8Value FromSerialized(byte[] serialized) =>
        TryCreate(serialized, out var value) ? value : throw new InvalidOperationException("The serializer produced bytes that are not one JSON value.");

    /// <summary>Copies the raw text of an object, array or number element, rewriting it when the element was parsed with comments or trailing commas.</summary>
    /// <param name="element">A defined element.</param>
    /// <returns>The value over a copy of the element text.</returns>
    /// <exception cref="InvalidOperationException">The element holds no JSON value, for example when it nests deeper than the validator allows.</exception>
    internal static JsonUtf8Value FromElement(JsonElement element)
    {
        if (TryCreateCopy(JsonMarshal.GetRawUtf8Value(element), out var value))
            return value;

        // Rare path: the document was parsed with lenient reader options, so the raw text may hold comments or trailing commas.
        var rewritten = new ArrayBufferWriter<byte>();
#pragma warning disable MA0045
        using (var writer = new Utf8JsonWriter(rewritten))
        {
            element.WriteTo(writer);
            writer.Flush();
        }
#pragma warning restore MA0045

        return TryCreateCopy(rewritten.WrittenSpan, out value) ? value : throw new InvalidOperationException("The JSON element does not hold one JSON value.");
    }

    /// <summary>Gets the value as a self-contained <see cref="JsonElement" />.</summary>
    /// <returns>A parsed copy of the value.</returns>
    internal JsonElement ToJsonElement()
    {
        var reader = new Utf8JsonReader(Bytes.Span);
        return JsonElement.ParseValue(ref reader);
    }
}
