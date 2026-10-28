using System;
using System.Buffers;
using System.Runtime.InteropServices;
using System.Text.Json;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Transport.Grpc;

/// <summary>Builds and reads <see cref="CacheValue" /> messages: the native kinds for scalars and one JSON text for everything structured.</summary>
internal static class CacheValueWire
{
    private static readonly JsonReaderOptions StrictReaderOptions = new()
    {
        CommentHandling = JsonCommentHandling.Disallow,
        AllowTrailingCommas = false,
    };

    /// <summary>Builds the wire form of a null value.</summary>
    /// <returns>A value of the null kind.</returns>
    internal static CacheValue Null() => new() { NullValue = NullValue.NullValue };

    /// <summary>Builds the wire form of a JSON element: a scalar element as its native kind, a number, an object or an array as its own text.</summary>
    /// <param name="element">The element.</param>
    /// <returns>The wire value; the text is copied out of the element document.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The element has an unknown kind.</exception>
    internal static CacheValue FromElement(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.Null or JsonValueKind.Undefined => Null(),
        JsonValueKind.True => new CacheValue { BoolValue = true },
        JsonValueKind.False => new CacheValue { BoolValue = false },
        JsonValueKind.String => new CacheValue { StringValue = element.GetString() ?? string.Empty },
        JsonValueKind.Number or JsonValueKind.Object or JsonValueKind.Array => new CacheValue { JsonUtf8 = ElementJson(element) },
        _ => throw new ArgumentOutOfRangeException(nameof(element), "Unsupported JSON value kind."),
    };

    /// <summary>Builds the wire form of JSON text without copying it.</summary>
    /// <param name="json">The JSON text; the caller never changes it again, because the message shares it.</param>
    /// <returns>A value of the JSON kind over <paramref name="json" />.</returns>
    internal static CacheValue FromJson(ReadOnlyMemory<byte> json) => new() { JsonUtf8 = UnsafeByteOperations.UnsafeWrap(json) };

    /// <summary>Writes a scalar wire value as JSON text, so a reader of another type can deserialize it.</summary>
    /// <param name="value">A value of a scalar kind or of the null kind.</param>
    /// <param name="destination">The destination of the JSON text.</param>
    /// <exception cref="ArgumentOutOfRangeException">The value has no scalar kind.</exception>
    internal static void WriteScalarJson(CacheValue value, IBufferWriter<byte> destination)
    {
        // Sync flush: the writer writes to memory; async Utf8JsonWriter disposal would allocate a state machine on every decoding.
#pragma warning disable MA0045
        using var writer = new Utf8JsonWriter(destination);
        switch (value.KindCase)
        {
            case CacheValue.KindOneofCase.StringValue:
                writer.WriteStringValue(value.StringValue);
                break;
            case CacheValue.KindOneofCase.BoolValue:
                writer.WriteBooleanValue(value.BoolValue);
                break;
            case CacheValue.KindOneofCase.Int64Value:
                writer.WriteNumberValue(value.Int64Value);
                break;
            case CacheValue.KindOneofCase.DoubleValue:
                writer.WriteNumberValue(value.DoubleValue);
                break;
            case CacheValue.KindOneofCase.NullValue:
                writer.WriteNullValue();
                break;
            case CacheValue.KindOneofCase.None:
            case CacheValue.KindOneofCase.JsonUtf8:
            default:
                throw new ArgumentOutOfRangeException(nameof(value), value.KindCase, "The cache value has no scalar kind.");
        }

        writer.Flush();
#pragma warning restore MA0045
    }

    /// <summary>Copies the text of an object, array or number element, rewriting it when the element was parsed with comments or trailing commas, which the wire form does not allow.</summary>
    /// <param name="element">The element.</param>
    /// <returns>The strict JSON text of the element.</returns>
    private static ByteString ElementJson(JsonElement element)
    {
        var raw = JsonMarshal.GetRawUtf8Value(element);
        if (IsStrictJson(raw))
            return ByteString.CopyFrom(raw);

        // Rare path: the document was parsed with lenient reader options, so the raw text may hold comments or trailing commas.
        var rewritten = new ArrayBufferWriter<byte>(raw.Length);
#pragma warning disable MA0045
        using (var writer = new Utf8JsonWriter(rewritten))
        {
            element.WriteTo(writer);
            writer.Flush();
        }
#pragma warning restore MA0045

        return ByteString.CopyFrom(rewritten.WrittenSpan);
    }

    private static bool IsStrictJson(ReadOnlySpan<byte> utf8)
    {
        try
        {
            var reader = new Utf8JsonReader(utf8, StrictReaderOptions);
            if (!reader.Read())
                return false;

            reader.Skip();
            return !reader.Read();
        }
        catch (JsonException)
        {
            return false;
        }
    }
}
