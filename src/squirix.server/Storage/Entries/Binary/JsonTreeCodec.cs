using System;
using System.IO;
using System.Text;
using System.Text.Json;

namespace Squirix.Server.Storage.Entries.Binary;

/// <summary>Encodes and decodes <see cref="JsonElement" /> trees without persisting UTF-8 JSON blobs.</summary>
internal static class JsonTreeCodec
{
    internal const int MaxUtf16StringLength = ushort.MaxValue;

    internal static bool TryRead(ReadOnlySpan<byte> source, out JsonElement element, out int bytesRead)
    {
        element = default;
        bytesRead = 0;
        if (!JsonTreeReadCodec.TryReadNode(source, out var node, out bytesRead))
            return false;

        element = node is null ? default : JsonSerializer.SerializeToElement(node, JsonTreeJsonContext.Default.JsonNode);
        return true;
    }

    internal static int Write(JsonElement element, Span<byte> destination) => JsonTreeWriteCodec.Write(element, destination);

    internal static int ComputeEncodedLength(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.Null or JsonValueKind.Undefined => 1,
        JsonValueKind.True or JsonValueKind.False => 2,
        JsonValueKind.String => 1 + 4 + Encoding.UTF8.GetByteCount(element.GetString() ?? string.Empty),
        JsonValueKind.Number => 1 + 8,
        JsonValueKind.Object => ComputeObjectLength(element),
        JsonValueKind.Array => ComputeArrayLength(element),
        _ => throw new InvalidDataException("Unsupported JSON value kind for binary tree encoding."),
    };

    private static int ComputeArrayLength(JsonElement element)
    {
        var length = 1 + 4;
        foreach (var item in element.EnumerateArray())
            length += ComputeEncodedLength(item);

        return length;
    }

    private static int ComputeObjectLength(JsonElement element)
    {
        var length = 1 + 2;
        foreach (var property in element.EnumerateObject())
        {
            var nameBytes = Encoding.UTF8.GetByteCount(property.Name);
            if (nameBytes > MaxUtf16StringLength)
                throw new InvalidDataException("Object property name exceeds maximum encoded length.");

            length += 2 + nameBytes + ComputeEncodedLength(property.Value);
        }

        return length;
    }
}
