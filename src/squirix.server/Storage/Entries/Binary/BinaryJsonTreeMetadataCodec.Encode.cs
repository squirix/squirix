using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Squirix.Server.Storage.Entries.Binary;

/// <summary>Single-pass metadata wire encoding helpers.</summary>
internal static partial class BinaryJsonTreeMetadataCodec
{
    private const int InitialEncodeCapacity = 512;

    internal static void Append(object? value, JsonTypeInfo typeInfo, ArrayBufferWriter<byte> buffer)
    {
        ArgumentNullException.ThrowIfNull(typeInfo);
        AppendValue(value, typeInfo, buffer);
    }

    internal static byte[] EncodeToOwned(object? value, JsonTypeInfo typeInfo)
    {
        ArgumentNullException.ThrowIfNull(typeInfo);
        var buffer = new ArrayBufferWriter<byte>(InitialEncodeCapacity);
        AppendValue(value, typeInfo, buffer);
#pragma warning disable ZA0302
        var owned = new byte[buffer.WrittenCount];
#pragma warning restore ZA0302
        buffer.WrittenSpan.CopyTo(owned);
        return owned;
    }

    private static void AppendDictionary(object value, JsonTypeInfo typeInfo, ArrayBufferWriter<byte> buffer)
    {
        ArgumentNullException.ThrowIfNull(value);
        if (value is not IDictionary dictionary)
            throw new InvalidOperationException("Value is not a dictionary.");

        var keyTypeInfo = ResolveKeyTypeInfo(typeInfo);
        var valueTypeInfo = ResolveElementTypeInfo(typeInfo);
        var kindSpan = buffer.GetSpan(1);
        kindSpan[0] = ValueKind.Object;
        buffer.Advance(1);
        var countOffset = buffer.WrittenCount;
        buffer.Advance(2);
        ushort entryCount = 0;
        foreach (DictionaryEntry entry in dictionary)
        {
            var keyText = FormatDictionaryKey(entry.Key, keyTypeInfo);
            var nameByteCount = 2 + Encoding.UTF8.GetByteCount(keyText);
            var nameSpan = buffer.GetSpan(nameByteCount);
            var nameWritten = WriteUtf8Prefixed(keyText, nameSpan);
            buffer.Advance(nameWritten);
            AppendValue(entry.Value, valueTypeInfo, buffer);
            entryCount++;
        }

        PatchUInt16(buffer, countOffset, entryCount);
    }

    private static void AppendEnumerable(object value, JsonTypeInfo typeInfo, ArrayBufferWriter<byte> buffer)
    {
        var elementTypeInfo = ResolveElementTypeInfo(typeInfo);
        if (value is not IEnumerable enumerable)
            throw new InvalidOperationException("Value is not enumerable.");

        var header = buffer.GetSpan(5);
        header[0] = ValueKind.Array;
        buffer.Advance(1);
        var countOffset = buffer.WrittenCount;
        buffer.Advance(4);
        uint itemCount = 0;
        foreach (var item in enumerable)
        {
            AppendValue(item, elementTypeInfo, buffer);
            itemCount++;
        }

        BinaryPrimitives.WriteUInt32LittleEndian(GetMutableWrittenSpan(buffer, countOffset, 4), itemCount);
    }

    private static void AppendLeaf(object value, JsonTypeInfo typeInfo, ArrayBufferWriter<byte> buffer)
    {
        if (!TryGetDirectLeafLength(value, out var length))
            throw new InvalidDataException($"Leaf type '{typeInfo.Type.Name}' is not supported by direct binary wire encoding.");

        var span = buffer.GetSpan(length);
        _ = WriteLeaf(value, typeInfo, span);
        buffer.Advance(length);
    }

    private static void AppendObject(object value, JsonTypeInfo typeInfo, ArrayBufferWriter<byte> buffer)
    {
        ArgumentNullException.ThrowIfNull(value);
        var kindSpan = buffer.GetSpan(1);
        kindSpan[0] = ValueKind.Object;
        buffer.Advance(1);
        var countOffset = buffer.WrittenCount;
        buffer.Advance(2);
        ushort propertyCount = 0;
        var propertyIndex = MetadataTypeInfoCache.GetObjectPropertyIndex(typeInfo);
        foreach (var entry in propertyIndex.Entries)
        {
            var property = entry.Property;
            if (!ShouldWriteProperty(property, value, typeInfo))
                continue;

            var nameByteCount = entry.PrefixedNameLength;
            var nameSpan = buffer.GetSpan(nameByteCount);
            var nameWritten = entry.WritePrefixedName(nameSpan);
            buffer.Advance(nameWritten);
            AppendValue(GetPropertyValue(property, value), ResolvePropertyTypeInfo(property, typeInfo), buffer);
            propertyCount++;
        }

        PatchUInt16(buffer, countOffset, propertyCount);
    }

    private static void AppendValue(object? value, JsonTypeInfo typeInfo, ArrayBufferWriter<byte> buffer)
    {
        if (value is null)
        {
            var span = buffer.GetSpan(1);
            span[0] = ValueKind.Null;
            buffer.Advance(1);
            return;
        }

        if (value is JsonElement jsonElement)
        {
            var jsonLength = BinaryJsonTreeCodec.ComputeEncodedLength(jsonElement);
            var jsonSpan = buffer.GetSpan(jsonLength);
            _ = BinaryJsonTreeCodec.Write(jsonElement, jsonSpan);
            buffer.Advance(jsonLength);
            return;
        }

        switch (typeInfo.Kind)
        {
            case JsonTypeInfoKind.Object:
                AppendObject(value, typeInfo, buffer);
                break;

            case JsonTypeInfoKind.Enumerable:
                AppendEnumerable(value, typeInfo, buffer);
                break;

            case JsonTypeInfoKind.Dictionary:
                AppendDictionary(value, typeInfo, buffer);
                break;
            default:
                AppendLeaf(value, typeInfo, buffer);
                break;
        }
    }

    private static Span<byte> GetMutableWrittenSpan(ArrayBufferWriter<byte> buffer, int offset, int length)
    {
        if (!MemoryMarshal.TryGetArray(buffer.WrittenMemory, out var segment) || segment.Array is null)
            throw new InvalidOperationException("Failed to patch encoded buffer.");

        return segment.Array.AsSpan(segment.Offset + offset, length);
    }

    private static void PatchUInt16(ArrayBufferWriter<byte> buffer, int offset, ushort value) =>
        BinaryPrimitives.WriteUInt16LittleEndian(GetMutableWrittenSpan(buffer, offset, 2), value);
}
