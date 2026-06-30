using System;
using System.Text.Json.Serialization.Metadata;
using Squirix.Serialization;

namespace Squirix.Internal.Cluster.Transport.Binary;

/// <summary>Resolves STJ metadata from <see cref="ISquirixSerializer" /> for wire encoding.</summary>
internal static class WireSerializerEx
{
    internal static JsonTypeInfo GetTypeInfo(ISquirixSerializer serializer, Type type)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(type);
        return MetadataTypeInfoCache.GetTypeInfo(ResolveSystemTextJsonSerializer(serializer).GetSerializerOptions(), type);
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
