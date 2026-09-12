using System;
using System.Runtime.CompilerServices;
using System.Text.Json.Serialization.Metadata;

namespace Squirix.Server.Core;

internal static class SerializerProvider
{
    internal static IServerSerializer Instance { get; } = CreateDefault();

    internal static T? Deserialize<T>(ReadOnlySpan<byte> payload, JsonTypeInfo<T>? typeInfo = null) => Instance.Deserialize(payload, typeInfo);

    private static IServerSerializer Create(IServerSerializer? serializer = null) => serializer ?? new ServerJsonSerializer();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IServerSerializer CreateDefault() => Create();
}
