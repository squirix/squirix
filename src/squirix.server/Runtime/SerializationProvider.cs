using System;
using System.Runtime.CompilerServices;

namespace Squirix.Server.Runtime;

internal static class SerializationProvider
{
    internal static IServerSerializer Instance { get; } = CreateDefault();

    internal static T? Deserialize<T>(ReadOnlySpan<byte> payload) => Instance.Deserialize<T>(payload);

    private static IServerSerializer Create(IServerSerializer? serializer = null) => serializer ?? new ServerJsonSerializer();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IServerSerializer CreateDefault() => Create();
}
