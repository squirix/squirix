using System;
using System.Text.Json.Serialization.Metadata;
using Squirix.Server.Serialization;

namespace Squirix.Server.Storage.Entries.Binary;

/// <summary>Resolves STJ metadata from the server serializer for wire encoding.</summary>
internal static class WireSerializerEx
{
    internal static JsonTypeInfo GetTypeInfo(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        return MetadataTypeInfoCache.GetTypeInfo(ResolveSystemTextJsonSerializer(SerializationProvider.Instance).GetSerializerOptions(), type);
    }

    private static SystemTextJsonSerializer ResolveSystemTextJsonSerializer(ISquirixSerializer serializer)
    {
        var current = serializer;
        while (current is MetricsDecoratedSerializer metricsDecoratedSerializer)
            current = metricsDecoratedSerializer.GetWireInner();

        if (current is SystemTextJsonSerializer systemTextJsonSerializer)
            return systemTextJsonSerializer;

        throw new InvalidOperationException("Wire metadata encoding requires SystemTextJsonSerializer.");
    }
}
