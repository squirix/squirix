using System;
using System.Runtime.CompilerServices;

namespace Squirix.Server.Serialization;

internal static class SerializationProvider
{
    internal static ISquirixSerializer Instance { get; } = CreateDefault();

    public static T? Deserialize<T>(ReadOnlySpan<byte> payload) => Instance.Deserialize<T>(payload);

    private static ISquirixSerializer Create(ISquirixSerializer? serializer = null, bool enableMetrics = true)
    {
        var effective = serializer ?? new SystemTextJsonSerializer();
        return enableMetrics ? EnsureMetrics(effective) : effective;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ISquirixSerializer CreateDefault() => Create();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ISquirixSerializer EnsureMetrics(ISquirixSerializer inner)
    {
        ArgumentNullException.ThrowIfNull(inner);
        return inner is MetricsDecoratedSerializer ? inner : new MetricsDecoratedSerializer(inner);
    }
}
