using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Squirix.Server.Storage.Entries.Binary;

/// <summary>Caches resolved <see cref="JsonTypeInfo" /> instances per serializer options.</summary>
internal static class MetadataTypeInfoCache
{
    private static readonly ConditionalWeakTable<JsonTypeInfo, ObjectPropertyIndex> PropertyIndexCache = new();
    private static readonly ConditionalWeakTable<JsonSerializerOptions, ConcurrentDictionary<Type, JsonTypeInfo>> TypeInfoCache = new();

    internal static JsonPropertyInfo? FindObjectProperty(JsonTypeInfo typeInfo, ReadOnlySpan<byte> utf8Name)
    {
        ArgumentNullException.ThrowIfNull(typeInfo);
        if (typeInfo.Kind is not JsonTypeInfoKind.Object)
            return null;

        return GetObjectPropertyIndex(typeInfo).Find(utf8Name);
    }

    internal static ObjectPropertyIndex GetObjectPropertyIndex(JsonTypeInfo typeInfo)
    {
        ArgumentNullException.ThrowIfNull(typeInfo);
        if (typeInfo.Kind is not JsonTypeInfoKind.Object)
            throw new InvalidOperationException("Property index requires object metadata.");

        return PropertyIndexCache.GetValue(typeInfo, static info => ObjectPropertyIndex.Create(info));
    }

    internal static JsonTypeInfo GetTypeInfo(JsonSerializerOptions options, Type type)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(type);
        var typeCache = TypeInfoCache.GetValue(options, static _ => new ConcurrentDictionary<Type, JsonTypeInfo>());
        return typeCache.GetOrAdd(type, static (resolvedType, serializerOptions) => serializerOptions.GetTypeInfo(resolvedType), options);
    }
}
