using System;
using System.Buffers.Binary;
using System.Collections;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace Squirix.Server.Storage.Entries.Binary;

/// <summary>Encodes and decodes arbitrary POCOs directly from binary tree wire values via STJ metadata.</summary>
internal static partial class BinaryJsonTreeMetadataCodec
{
    private const int MaxUtf16StringLength = ushort.MaxValue;

    internal static int ComputeEncodedLength(object? value, JsonTypeInfo typeInfo)
    {
        ArgumentNullException.ThrowIfNull(typeInfo);
        if (value is null)
            return 1;

        if (value is JsonElement jsonElement)
            return BinaryJsonTreeCodec.ComputeEncodedLength(jsonElement);

        return typeInfo.Kind switch
        {
            JsonTypeInfoKind.Object => ComputeObjectLength(value, typeInfo),
            JsonTypeInfoKind.Enumerable => ComputeEnumerableLength(value, typeInfo),
            JsonTypeInfoKind.Dictionary => ComputeDictionaryLength(value, typeInfo),
            _ => ComputeLeafLength(value, typeInfo),
        };
    }

    internal static bool TryRead<T>(ReadOnlySpan<byte> source, JsonTypeInfo typeInfo, out T? value)
    {
        value = default;
        if (!TryReadValue(source, typeInfo, out var decoded, out _))
            return false;

        if (decoded is null)
            return true;

        if (decoded is not T typed)
            return false;
        value = typed;
        return true;
    }

    internal static int Write(object? value, JsonTypeInfo typeInfo, Span<byte> destination)
    {
        ArgumentNullException.ThrowIfNull(typeInfo);
        if (value is null)
            return WriteNull(destination);

        if (value is JsonElement jsonElement)
            return BinaryJsonTreeCodec.Write(jsonElement, destination);

        return typeInfo.Kind switch
        {
            JsonTypeInfoKind.Object => WriteObject(value, typeInfo, destination),
            JsonTypeInfoKind.Enumerable => WriteEnumerable(value, typeInfo, destination),
            JsonTypeInfoKind.Dictionary => WriteDictionary(value, typeInfo, destination),
            _ => WriteLeaf(value, typeInfo, destination),
        };
    }

    private static int ComputeDictionaryLength(object value, JsonTypeInfo typeInfo)
    {
        var keyTypeInfo = ResolveKeyTypeInfo(typeInfo);
        var valueTypeInfo = ResolveElementTypeInfo(typeInfo);
        if (value is not IDictionary dictionary)
            throw new InvalidOperationException("Value is not a dictionary.");

        var length = 1 + 2;
        foreach (DictionaryEntry entry in dictionary)
        {
            length += 2 + Encoding.UTF8.GetByteCount(FormatDictionaryKey(entry.Key, keyTypeInfo));
            length += ComputeEncodedLength(entry.Value, valueTypeInfo);
        }

        return length;
    }

    private static int ComputeEnumerableLength(object value, JsonTypeInfo typeInfo)
    {
        var length = 1 + 4;
        var elementTypeInfo = ResolveElementTypeInfo(typeInfo);
        if (value is not IEnumerable enumerable)
            throw new InvalidOperationException("Value is not enumerable.");

        foreach (var item in enumerable)
            length += ComputeEncodedLength(item, elementTypeInfo);

        return length;
    }

    private static int ComputeLeafLength(object value, JsonTypeInfo typeInfo)
    {
        if (TryGetDirectLeafLength(value, out var length))
            return length;

        throw new InvalidDataException($"Leaf type '{typeInfo.Type.Name}' is not supported by direct binary wire encoding.");
    }

    private static int ComputeObjectLength(object value, JsonTypeInfo typeInfo)
    {
        ArgumentNullException.ThrowIfNull(value);
        var length = 1 + 2;
        var propertyIndex = MetadataTypeInfoCache.GetObjectPropertyIndex(typeInfo);
        foreach (var entry in propertyIndex.Entries)
        {
            var property = entry.Property;
            if (!ShouldWriteProperty(property, value, typeInfo))
                continue;

            length += entry.PrefixedNameLength;
            length += ComputeEncodedLength(GetPropertyValue(property, value), ResolvePropertyTypeInfo(property, typeInfo));
        }

        return length;
    }

    private static JsonPropertyInfo? FindProperty(JsonTypeInfo typeInfo, ReadOnlySpan<byte> utf8Name) => MetadataTypeInfoCache.FindObjectProperty(typeInfo, utf8Name);

    private static string FormatDictionaryKey(object key, JsonTypeInfo keyTypeInfo)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (keyTypeInfo.Type != typeof(string))
            throw new InvalidDataException($"Dictionary key type '{keyTypeInfo.Type.Name}' is not supported by direct binary wire encoding.");
        if (key is string text)
            return text;

        throw new InvalidDataException("Dictionary key is not a string.");
    }

    private static object? GetPropertyValue(JsonPropertyInfo property, object instance)
    {
        if (property.Get is not { } get)
            throw new InvalidOperationException($"Property '{property.Name}' is missing a getter.");

        return get(instance);
    }

    private static string ParseDictionaryKey(ReadOnlySpan<byte> utf8Key, JsonTypeInfo keyTypeInfo)
    {
        if (keyTypeInfo.Type == typeof(string))
            return Encoding.UTF8.GetString(utf8Key);

        throw new InvalidDataException($"Dictionary key type '{keyTypeInfo.Type.Name}' is not supported by direct binary wire encoding.");
    }

    private static JsonTypeInfo ResolveElementTypeInfo(JsonTypeInfo typeInfo)
    {
        ArgumentNullException.ThrowIfNull(typeInfo);
        ArgumentNullException.ThrowIfNull(typeInfo.Options);
        if (typeInfo.ElementType is not { } elementType)
            throw new InvalidOperationException("Enumerable metadata is missing element type.");

        return MetadataTypeInfoCache.GetTypeInfo(typeInfo.Options, elementType);
    }

    private static JsonTypeInfo ResolveKeyTypeInfo(JsonTypeInfo typeInfo)
    {
        ArgumentNullException.ThrowIfNull(typeInfo);
        ArgumentNullException.ThrowIfNull(typeInfo.Options);
        if (typeInfo.KeyType is not { } keyType)
            throw new InvalidOperationException("Dictionary metadata is missing key type.");

        return MetadataTypeInfoCache.GetTypeInfo(typeInfo.Options, keyType);
    }

    private static JsonTypeInfo ResolvePropertyTypeInfo(JsonPropertyInfo property, JsonTypeInfo parent)
    {
        ArgumentNullException.ThrowIfNull(property);
        ArgumentNullException.ThrowIfNull(parent);
        ArgumentNullException.ThrowIfNull(parent.Options);
        return MetadataTypeInfoCache.GetTypeInfo(parent.Options, property.PropertyType);
    }

    private static void SetPropertyValue(JsonPropertyInfo property, object instance, object? propertyValue)
    {
        if (property.Set is not { } set)
            throw new InvalidOperationException($"Property '{property.Name}' is missing a setter.");

        set(instance, propertyValue);
    }

    private static bool ShouldWriteProperty(JsonPropertyInfo property, object value, JsonTypeInfo typeInfo)
    {
        ArgumentNullException.ThrowIfNull(property);
        ArgumentNullException.ThrowIfNull(typeInfo.Options);
        if (typeInfo.Options.DefaultIgnoreCondition is not JsonIgnoreCondition.WhenWritingNull)
            return true;

        return GetPropertyValue(property, value) is not null;
    }

    private static bool TryGetDirectLeafLength(object value, out int length)
    {
        length = 0;
        switch (value)
        {
            case bool:
                length = 2;
                return true;
            case string s:
                length = 1 + 4 + Encoding.UTF8.GetByteCount(s);
                return true;
            case byte[] bytes:
                length = 1 + 4 + bytes.Length;
                return true;
            case sbyte or byte or short or ushort or int or uint or long:
            case float or double:
                length = 1 + 8;
                return true;
            case decimal m:
                length = 1 + 2 + Encoding.UTF8.GetByteCount(m.ToString(CultureInfo.InvariantCulture));
                return true;
            case DateTimeOffset:
                length = 1 + 8;
                return true;
            default:
                if (!value.GetType().IsEnum)
                    return false;
                length = 1 + 8;
                return true;
        }
    }

    private static bool TryReadArray(ReadOnlySpan<byte> source, JsonTypeInfo typeInfo, out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 0;
        if (source.Length < 1 + 4 || source[0] is not ValueKind.Array)
            return false;

        var elementTypeInfo = ResolveElementTypeInfo(typeInfo);
        var count = BinaryPrimitives.ReadUInt32LittleEndian(source[1..]);
        bytesRead = 5;
        if (typeInfo.Type.IsArray)
        {
            var items = Array.CreateInstance(elementTypeInfo.Type, int.CreateChecked(count));
            for (var i = 0; i < count; i++)
            {
                if (!TryReadValue(source[bytesRead..], elementTypeInfo, out var item, out var itemBytes))
                    return false;

                bytesRead += itemBytes;
                items.SetValue(item, i);
            }

            value = items;
            return true;
        }

        if (typeInfo.CreateObject is { } createObject && createObject() is IList list)
        {
            for (var i = 0; i < count; i++)
            {
                if (!TryReadValue(source[bytesRead..], elementTypeInfo, out var item, out var itemBytes))
                    return false;

                bytesRead += itemBytes;
                _ = list.Add(item);
            }

            value = list;
            return true;
        }

        var arrayItems = Array.CreateInstance(elementTypeInfo.Type, int.CreateChecked(count));
        for (var i = 0; i < count; i++)
        {
            if (!TryReadValue(source[bytesRead..], elementTypeInfo, out var item, out var itemBytes))
                return false;

            bytesRead += itemBytes;
            arrayItems.SetValue(item, i);
        }

        value = arrayItems;
        return true;
    }

    private static bool TryReadBool(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 0;
        if (source.Length < 2)
            return false;

        value = source[1] is not 0;
        bytesRead = 2;
        return true;
    }

    private static bool TryReadBytes(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 0;
        if (!TryReadUtf32Prefixed(source[1..], out var rawBytes, out var rawBytesRead))
            return false;

#pragma warning disable ZA0302
        var bytes = new byte[rawBytes.Length];
#pragma warning restore ZA0302
        rawBytes.CopyTo(bytes);
        value = bytes;
        bytesRead = 1 + rawBytesRead;
        return true;
    }

    private static bool TryReadDateTimeOffset(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 0;
        if (!TryReadInt64(source, out var int64Value, out bytesRead))
            return false;

        if (int64Value is not long unixMilliseconds)
            return false;

        value = DateTimeOffset.FromUnixTimeMilliseconds(unixMilliseconds);
        return true;
    }

    private static bool TryReadDecimal(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 0;
        if (!TryReadUtf8Prefixed(source[1..], out var decimalText, out var decimalBytesRead))
            return false;

        if (!decimal.TryParse(decimalText, NumberStyles.Float, CultureInfo.InvariantCulture, out var decimalValue))
            return false;

        value = decimalValue;
        bytesRead = 1 + decimalBytesRead;
        return true;
    }

    private static bool TryReadDecimalFromDouble(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
    {
        if (!TryReadDouble(source, out var doubleValue, out bytesRead) || doubleValue is not double doubleNumber)
        {
            value = null;
            bytesRead = 0;
            return false;
        }

        value = Convert.ToDecimal(doubleNumber, CultureInfo.InvariantCulture);
        return true;
    }

    private static bool TryReadDecimalFromInt64(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
    {
        if (!TryReadInt64(source, out var int64Value, out bytesRead) || int64Value is not long integer)
        {
            value = null;
            bytesRead = 0;
            return false;
        }

        value = Convert.ToDecimal(integer, CultureInfo.InvariantCulture);
        return true;
    }

    private static bool TryReadDictionary(ReadOnlySpan<byte> source, JsonTypeInfo typeInfo, out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 0;
        if (source.Length < 1 + 2 || source[0] is not ValueKind.Object)
            return false;

        if (typeInfo.CreateObject is not { } createObject || createObject() is not IDictionary dictionary)
            return false;

        var keyTypeInfo = ResolveKeyTypeInfo(typeInfo);
        var valueTypeInfo = ResolveElementTypeInfo(typeInfo);
        var entryCount = BinaryPrimitives.ReadUInt16LittleEndian(source[1..]);
        var offset = 3;
        for (var i = 0; i < entryCount; i++)
        {
            if (!TryReadUtf8PrefixedSpan(source[offset..], out var keyBytes, out var keyBytesRead))
                return false;

            offset += keyBytesRead;
            if (!TryReadValue(source[offset..], valueTypeInfo, out var entryValue, out var valueBytesRead))
                return false;

            offset += valueBytesRead;
            dictionary.Add(ParseDictionaryKey(keyBytes, keyTypeInfo), entryValue);
        }

        value = dictionary;
        bytesRead = offset;
        return true;
    }

    private static bool TryReadDirectLeaf(ReadOnlySpan<byte> source, JsonTypeInfo typeInfo, out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 0;
        var targetType = typeInfo.Type;
        switch (source[0])
        {
            case ValueKind.Null:
                return TryReadNull(out value, out bytesRead);

            case ValueKind.Bool when targetType == typeof(bool):
                return TryReadBool(source, out value, out bytesRead);

            case ValueKind.String when targetType == typeof(string):
                return TryReadString(source, out value, out bytesRead);

            case ValueKind.Bytes when targetType == typeof(byte[]):
                return TryReadBytes(source, out value, out bytesRead);

            case ValueKind.Int64 when targetType == typeof(DateTimeOffset):
            case ValueKind.Int64 when targetType == typeof(long):
            case ValueKind.Int64 when targetType == typeof(int):
                return TryReadInt64TypedLeaf(source, targetType, out value, out bytesRead);

            case ValueKind.Double when targetType == typeof(double):
                return TryReadDouble(source, out value, out bytesRead);

            case ValueKind.Double when targetType == typeof(float):
                if (!TryReadDouble(source, out var doubleValue, out bytesRead))
                    return false;

                if (doubleValue is not double doubleNumber)
                    return false;

                value = Convert.ToSingle(doubleNumber, CultureInfo.InvariantCulture);
                return true;

            case ValueKind.Decimal when targetType == typeof(decimal):
                return TryReadDecimal(source, out value, out bytesRead);

            case ValueKind.Double when targetType == typeof(decimal):
                return TryReadDecimalFromDouble(source, out value, out bytesRead);

            case ValueKind.Int64 when targetType == typeof(decimal):
                return TryReadDecimalFromInt64(source, out value, out bytesRead);

            case ValueKind.Int64 when targetType.IsEnum:
                if (!TryReadInt64(source, out var enumInt64, out bytesRead) || enumInt64 is not long enumValue)
                    return false;

                value = Enum.ToObject(targetType, enumValue);
                return true;

            default:
                return false;
        }
    }

    private static bool TryReadDouble(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 0;
        if (source.Length < 1 + 8)
            return false;

        value = BinaryPrimitives.ReadDoubleLittleEndian(source[1..]);
        bytesRead = 1 + 8;
        return true;
    }

    private static bool TryReadInt64(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 0;
        if (source.Length < 1 + 8)
            return false;

        value = BinaryPrimitives.ReadInt64LittleEndian(source[1..]);
        bytesRead = 1 + 8;
        return true;
    }

    private static bool TryReadInt64TypedLeaf(ReadOnlySpan<byte> source, Type targetType, out object? value, out int bytesRead)
    {
        if (targetType == typeof(DateTimeOffset))
            return TryReadDateTimeOffset(source, out value, out bytesRead);

        if (targetType == typeof(long))
            return TryReadInt64(source, out value, out bytesRead);

        if (targetType != typeof(int))
        {
            value = null;
            bytesRead = 0;
            return false;
        }

        if (!TryReadInt64(source, out var int64Value, out bytesRead) || int64Value is not long longValue)
        {
            value = null;
            return false;
        }

        value = int.CreateChecked(longValue);
        return true;
    }

    private static bool TryReadLeaf(ReadOnlySpan<byte> source, JsonTypeInfo typeInfo, out object? value, out int bytesRead) =>
        TryReadDirectLeaf(source, typeInfo, out value, out bytesRead);

    private static bool TryReadNull(out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 1;
        return true;
    }

    private static bool TryReadObject(ReadOnlySpan<byte> source, JsonTypeInfo typeInfo, out object? value, out int bytesRead)
    {
        ArgumentNullException.ThrowIfNull(typeInfo);
        value = null;
        bytesRead = 0;
        if (source.Length < 1 + 2 || source[0] is not ValueKind.Object)
            return false;

        return TryReadObjectViaSetters(source, typeInfo, out value, out bytesRead);
    }

    private static bool TryReadObjectProperty(ReadOnlySpan<byte> source, JsonTypeInfo typeInfo, out JsonPropertyInfo? jsonProperty, out object? propertyValue, out int bytesRead)
    {
        jsonProperty = null;
        propertyValue = null;
        bytesRead = 0;
        if (!TryReadUtf8PrefixedSpan(source, out var name, out var nameBytes))
            return false;

        bytesRead = nameBytes;
        jsonProperty = FindProperty(typeInfo, name);
        if (jsonProperty is null)
        {
            if (!TrySkipValue(source[bytesRead..], out var skipBytes))
                return false;

            bytesRead += skipBytes;
            return true;
        }

        if (!TryReadValue(source[bytesRead..], ResolvePropertyTypeInfo(jsonProperty, typeInfo), out propertyValue, out var valueBytes))
            return false;

        bytesRead += valueBytes;
        return true;
    }

    private static bool TryReadObjectViaSetters(ReadOnlySpan<byte> source, JsonTypeInfo typeInfo, out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 0;
        object instance;
        if (typeInfo.CreateObject is { } createObject)
        {
            if (createObject() is not { } created)
                return false;

            instance = created;
        }
        else
        {
            instance = RuntimeHelpers.GetUninitializedObject(typeInfo.Type);
        }

        var propertyCount = BinaryPrimitives.ReadUInt16LittleEndian(source[1..]);
        var offset = 3;
        for (var i = 0; i < propertyCount; i++)
        {
            if (!TryReadObjectProperty(source[offset..], typeInfo, out var jsonProperty, out var propertyValue, out var propertyBytes))
                return false;

            offset += propertyBytes;
            if (jsonProperty is null)
                continue;

            SetPropertyValue(jsonProperty, instance, propertyValue);
        }

        value = instance;
        bytesRead = offset;
        return true;
    }

    private static bool TryReadString(ReadOnlySpan<byte> source, out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 0;
        if (!TryReadUtf32Prefixed(source[1..], out var stringBytes, out var stringBytesRead))
            return false;

        value = Encoding.UTF8.GetString(stringBytes);
        bytesRead = 1 + stringBytesRead;
        return true;
    }

    private static bool TryReadUtf32Prefixed(ReadOnlySpan<byte> source, out ReadOnlySpan<byte> bytes, out int bytesRead)
    {
        bytes = default;
        bytesRead = 0;
        if (source.Length < 4)
            return false;

        var length = BinaryPrimitives.ReadUInt32LittleEndian(source);
        var lengthInt = int.CreateChecked(length);
        bytesRead = 4 + lengthInt;
        if (source.Length < bytesRead)
            return false;

        bytes = source.Slice(4, lengthInt);
        return true;
    }

    private static bool TryReadUtf8Prefixed(ReadOnlySpan<byte> source, out string text, out int bytesRead)
    {
        text = string.Empty;
        bytesRead = 0;
        if (source.Length < 2)
            return false;

        var length = BinaryPrimitives.ReadUInt16LittleEndian(source);
        bytesRead = 2 + length;
        if (source.Length < bytesRead)
            return false;

        text = Encoding.UTF8.GetString(source.Slice(2, length));
        return true;
    }

    private static bool TryReadUtf8PrefixedSpan(ReadOnlySpan<byte> source, out ReadOnlySpan<byte> text, out int bytesRead)
    {
        text = default;
        bytesRead = 0;
        if (source.Length < 2)
            return false;

        var length = BinaryPrimitives.ReadUInt16LittleEndian(source);
        bytesRead = 2 + length;
        if (source.Length < bytesRead)
            return false;

        text = source.Slice(2, length);
        return true;
    }

    private static bool TryReadValue(ReadOnlySpan<byte> source, JsonTypeInfo typeInfo, out object? value, out int bytesRead)
    {
        value = null;
        bytesRead = 0;
        if (source.IsEmpty)
            return false;

        if (source[0] is ValueKind.Null)
            return TryReadNull(out value, out bytesRead);

        if (typeInfo.Kind is JsonTypeInfoKind.Object && source[0] is ValueKind.Object)
            return TryReadObject(source, typeInfo, out value, out bytesRead);

        if (typeInfo.Kind is JsonTypeInfoKind.Dictionary && source[0] is ValueKind.Object)
            return TryReadDictionary(source, typeInfo, out value, out bytesRead);

        if (typeInfo.Kind is JsonTypeInfoKind.Enumerable && source[0] is ValueKind.Array)
            return TryReadArray(source, typeInfo, out value, out bytesRead);

        return TryReadLeaf(source, typeInfo, out value, out bytesRead);
    }

    private static bool TrySkipArray(ReadOnlySpan<byte> source, out int bytesRead)
    {
        bytesRead = 0;
        if (source.Length < 1 + 4)
            return false;

        var count = BinaryPrimitives.ReadUInt32LittleEndian(source[1..]);
        bytesRead = 5;
        for (var i = 0; i < count; i++)
        {
            if (!TrySkipValue(source[bytesRead..], out var itemBytes))
                return false;

            bytesRead += itemBytes;
        }

        return true;
    }

    private static bool TrySkipObject(ReadOnlySpan<byte> source, out int bytesRead)
    {
        bytesRead = 0;
        if (source.Length < 1 + 2)
            return false;

        var count = BinaryPrimitives.ReadUInt16LittleEndian(source[1..]);
        bytesRead = 3;
        for (var i = 0; i < count; i++)
        {
            if (!TryReadUtf8PrefixedSpan(source[bytesRead..], out _, out var nameBytes))
                return false;

            bytesRead += nameBytes;
            if (!TrySkipValue(source[bytesRead..], out var valueBytes))
                return false;

            bytesRead += valueBytes;
        }

        return true;
    }

    private static bool TrySkipValue(ReadOnlySpan<byte> source, out int bytesRead)
    {
        bytesRead = 0;
        if (source.IsEmpty)
            return false;

        switch (source[0])
        {
            case ValueKind.Null:
                bytesRead = 1;
                return true;

            case ValueKind.Bool:
                bytesRead = 2;
                return source.Length >= bytesRead;

            case ValueKind.String:
            case ValueKind.Bytes:
                if (!TryReadUtf32Prefixed(source[1..], out _, out var prefixedBytesRead))
                    return false;

                bytesRead = 1 + prefixedBytesRead;
                return true;

            case ValueKind.Int64:
            case ValueKind.Double:
                bytesRead = 1 + 8;
                return source.Length >= bytesRead;

            case ValueKind.Decimal:
                if (!TryReadUtf8Prefixed(source[1..], out _, out var decimalBytesRead))
                    return false;

                bytesRead = 1 + decimalBytesRead;
                return true;

            case ValueKind.Object:
                return TrySkipObject(source, out bytesRead);

            case ValueKind.Array:
                return TrySkipArray(source, out bytesRead);

            default:
                return false;
        }
    }

    private static bool TryWriteDirectLeaf(object value, JsonTypeInfo typeInfo, Span<byte> destination, out int bytesWritten)
    {
        bytesWritten = 0;
        var offset = 0;
        switch (value)
        {
            case bool boolean:
                destination[offset++] = ValueKind.Bool;
                if (boolean)
                    destination[offset++] = 1;
                else
                    destination[offset++] = 0;

                bytesWritten = offset;
                return true;

            case string text:
                destination[offset++] = ValueKind.String;
                offset += WriteUtf32PrefixedString(text, destination[offset..]);
                bytesWritten = offset;
                return true;

            case byte[] bytes:
                destination[offset++] = ValueKind.Bytes;
                offset += WriteUtf32Prefixed(bytes, destination[offset..]);
                bytesWritten = offset;
                return true;

            case sbyte or byte or short or ushort or int or uint or long:
                destination[offset++] = ValueKind.Int64;
                BinaryPrimitives.WriteInt64LittleEndian(destination[offset..], Convert.ToInt64(value, CultureInfo.InvariantCulture));
                bytesWritten = offset + 8;
                return true;

            case float or double:
                destination[offset++] = ValueKind.Double;
                BinaryPrimitives.WriteDoubleLittleEndian(destination[offset..], Convert.ToDouble(value, CultureInfo.InvariantCulture));
                bytesWritten = offset + 8;
                return true;

            case decimal decimalValue:
                destination[offset++] = ValueKind.Decimal;
                offset += WriteUtf8Prefixed(decimalValue.ToString(CultureInfo.InvariantCulture), destination[offset..]);
                bytesWritten = offset;
                return true;

            case DateTimeOffset dateTimeOffset:
                bytesWritten = WriteDateTimeOffsetLeaf(dateTimeOffset, destination);
                return true;

            default:
                return TryWriteEnumLeaf(value, typeInfo, destination, out bytesWritten);
        }
    }

    private static bool TryWriteEnumLeaf(object value, JsonTypeInfo typeInfo, Span<byte> destination, out int bytesWritten)
    {
        bytesWritten = 0;
        if (!typeInfo.Type.IsEnum)
            return false;

        var offset = 0;
        destination[offset++] = ValueKind.Int64;
        BinaryPrimitives.WriteInt64LittleEndian(destination[offset..], Convert.ToInt64(value, CultureInfo.InvariantCulture));
        bytesWritten = offset + 8;
        return true;
    }

    private static int WriteDateTimeOffsetLeaf(DateTimeOffset value, Span<byte> destination)
    {
        destination[0] = ValueKind.Int64;
        BinaryPrimitives.WriteInt64LittleEndian(destination[1..], value.ToUnixTimeMilliseconds());
        return 1 + 8;
    }

    private static int WriteDictionary(object value, JsonTypeInfo typeInfo, Span<byte> destination)
    {
        ArgumentNullException.ThrowIfNull(value);
        if (value is not IDictionary dictionary)
            throw new InvalidOperationException("Value is not a dictionary.");

        var keyTypeInfo = ResolveKeyTypeInfo(typeInfo);
        var valueTypeInfo = ResolveElementTypeInfo(typeInfo);
        var offset = 0;
        destination[offset++] = ValueKind.Object;
        var countOffset = offset;
        offset += 2;
        ushort entryCount = 0;
        foreach (DictionaryEntry entry in dictionary)
        {
            offset += WriteUtf8Prefixed(FormatDictionaryKey(entry.Key, keyTypeInfo), destination[offset..]);
            offset += Write(entry.Value, valueTypeInfo, destination[offset..]);
            entryCount++;
        }

        BinaryPrimitives.WriteUInt16LittleEndian(destination[countOffset..], entryCount);
        return offset;
    }

    private static int WriteEnumerable(object value, JsonTypeInfo typeInfo, Span<byte> destination)
    {
        var elementTypeInfo = ResolveElementTypeInfo(typeInfo);
        if (value is not IEnumerable enumerable)
            throw new InvalidOperationException("Value is not enumerable.");

        var offset = 0;
        destination[offset++] = ValueKind.Array;
        var countOffset = offset;
        offset += 4;
        uint itemCount = 0;
        foreach (var item in enumerable)
        {
            offset += Write(item, elementTypeInfo, destination[offset..]);
            itemCount++;
        }

        BinaryPrimitives.WriteUInt32LittleEndian(destination[countOffset..], itemCount);
        return offset;
    }

    private static int WriteLeaf(object value, JsonTypeInfo typeInfo, Span<byte> destination)
    {
        if (TryWriteDirectLeaf(value, typeInfo, destination, out var bytesWritten))
            return bytesWritten;

        throw new InvalidDataException($"Leaf type '{typeInfo.Type.Name}' is not supported by direct binary wire encoding.");
    }

    private static int WriteNull(Span<byte> destination)
    {
        destination[0] = ValueKind.Null;
        return 1;
    }

    private static int WriteObject(object value, JsonTypeInfo typeInfo, Span<byte> destination)
    {
        ArgumentNullException.ThrowIfNull(value);
        var offset = 0;
        destination[offset++] = ValueKind.Object;
        var countOffset = offset;
        offset += 2;
        ushort propertyCount = 0;
        foreach (var property in typeInfo.Properties)
        {
            if (!ShouldWriteProperty(property, value, typeInfo))
                continue;

            offset += WriteUtf8Prefixed(property.Name, destination[offset..]);
            offset += Write(GetPropertyValue(property, value), ResolvePropertyTypeInfo(property, typeInfo), destination[offset..]);
            propertyCount++;
        }

        BinaryPrimitives.WriteUInt16LittleEndian(destination[countOffset..], propertyCount);
        return offset;
    }

    private static int WriteUtf32Prefixed(ReadOnlySpan<byte> bytes, Span<byte> destination)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(destination, uint.CreateTruncating(bytes.Length));
        bytes.CopyTo(destination[4..]);
        return 4 + bytes.Length;
    }

    private static int WriteUtf32PrefixedString(string text, Span<byte> destination)
    {
        var byteCount = Encoding.UTF8.GetByteCount(text);
        BinaryPrimitives.WriteUInt32LittleEndian(destination, uint.CreateTruncating(byteCount));
        _ = Encoding.UTF8.GetBytes(text, destination[4..]);
        return 4 + byteCount;
    }

    private static int WriteUtf8Prefixed(string text, Span<byte> destination)
    {
        var byteCount = Encoding.UTF8.GetByteCount(text);
        if (byteCount > MaxUtf16StringLength)
            throw new InvalidDataException("Object property name exceeds maximum encoded length.");

        BinaryPrimitives.WriteUInt16LittleEndian(destination, ushort.CreateTruncating(byteCount));
        _ = Encoding.UTF8.GetBytes(text, destination[2..]);
        return 2 + byteCount;
    }
}
