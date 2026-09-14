using System;
using System.Collections.Concurrent;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using System.Threading;

namespace Squirix.Server.Core;

/// <summary>Resolves AOT-safe JSON metadata from registered source-generation contexts.</summary>
/// <remarks>
/// The chain is seeded by the hosting layer, never by Core: the hosting entry point registers
/// its context at startup (and unit tests via module initializer), while hosts and tests add
/// their own contexts via <see cref="RegisterContext" /> so closed payload types resolve
/// without reflection. Core must not name hosting-layer contexts directly, otherwise the
/// low-level Squirix.Server.Core namespace would use the higher-level Squirix.Server
/// namespace (ND1400). Serialization resolves by runtime type first, so base/interface-declared
/// entries keep derived properties; deserialization resolves by declared type.
/// Reads are lock-free: registrations are rare (startup) and swap the snapshot array,
/// while resolved <see cref="JsonTypeInfo" /> instances are cached per <see cref="Type" />.
/// The snapshot array field is volatile, so readers observe it with a single acquiring;
/// Volatile.Read(ref ...) is intentionally not used because passing a volatile field
/// by reference voids its volatility (CS0420).
/// <para>
/// This engine intentionally mirrors the client-side metadata chain: the client and server
/// assemblies must not reference each other, so the chain cannot be shared. Keep the two in sync.
/// </para>
/// </remarks>
internal static class ServerSerializerMetadata
{
    private static readonly Lock Sync = new();
    private static readonly ConcurrentDictionary<Type, JsonTypeInfo> InfoByType = new();
    private static readonly ContextsSnapshot Contexts = new([]);

    /// <summary>Adds a source-generation context to the resolution chain.</summary>
    /// <param name="context">Context to consult after the already registered ones.</param>
    internal static void RegisterContext(JsonSerializerContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        lock (Sync)
        {
            if (Array.IndexOf(Contexts.Value, context) >= 0)
                return;
            Contexts.Value = [.. Contexts.Value, context];
            InfoByType.Clear();
        }
    }

    /// <summary>Resolves metadata for <typeparamref name="T" /> or throws.</summary>
    /// <typeparam name="T">Declared type to resolve.</typeparam>
    /// <returns>Source-generated metadata for the type.</returns>
    /// <exception cref="InvalidOperationException">Thrown when no registered context covers the type.</exception>
    internal static JsonTypeInfo<T> Resolve<T>()
    {
        var declaredType = typeof(T);
        if (InfoByType.TryGetValue(declaredType, out var cached) && cached is JsonTypeInfo<T> typed)
            return typed;

        var snapshot = Contexts.Value;
        for (var index = 0; index < snapshot.Length; index++)
        {
            if (snapshot[index].GetTypeInfo(declaredType) is not JsonTypeInfo<T> found)
                continue;
            _ = InfoByType.TryAdd(declaredType, found);
            return found;
        }

        return ThrowMissingMetadata<T>();
    }

    /// <summary>Serializes a value using runtime-type metadata when it differs from the declared type.</summary>
    /// <param name="destination">Stream receiving the payload.</param>
    /// <param name="value">Value to serialize.</param>
    /// <param name="declaredType">Declared compile-time type.</param>
    internal static void Serialize(Stream destination, object? value, Type declaredType)
    {
        var info = ResolveByEffectiveType(value, declaredType);

        // Sync by IServerSerializer contract: callers pass already-buffered destinations,
        // an async state machine per journal writing would be pure overhead.
#pragma warning disable MA0045
        JsonSerializer.Serialize(destination, value, info);
#pragma warning restore MA0045
    }

    /// <summary>Serializes a value to a <see cref="JsonElement" /> using runtime-type metadata when it differs.</summary>
    /// <param name="value">Value to serialize.</param>
    /// <param name="declaredType">Declared compile-time type.</param>
    /// <returns>Serialized element.</returns>
    internal static JsonElement SerializeToElement(object? value, Type declaredType)
    {
        var info = ResolveByEffectiveType(value, declaredType);
        return JsonSerializer.SerializeToElement(value, info);
    }

    /// <summary>Serializes a value to UTF-8 bytes using runtime-type metadata when it differs.</summary>
    /// <param name="value">Value to serialize.</param>
    /// <param name="declaredType">Declared compile-time type.</param>
    /// <returns>UTF-8 payload.</returns>
    internal static byte[] SerializeToUtf8Bytes(object? value, Type declaredType)
    {
        var info = ResolveByEffectiveType(value, declaredType);
        return JsonSerializer.SerializeToUtf8Bytes(value, info);
    }

    private static JsonTypeInfo ResolveByEffectiveType(object? value, Type declaredType)
    {
        var runtimeType = value?.GetType();
        return runtimeType != null && runtimeType != declaredType
            ? GetOrResolve(
                runtimeType,
                $"No JSON metadata registered for {runtimeType} (runtime type of {declaredType}). Register a source-generated JsonSerializerContext covering this type (AOT requirement).")
            : GetOrResolve(
                declaredType,
                $"No JSON metadata registered for {declaredType}. Register a source-generated JsonSerializerContext covering this type (AOT requirement).");
    }

    private static JsonTypeInfo GetOrResolve(Type type, string missingMessage)
    {
        if (InfoByType.TryGetValue(type, out var cached))
            return cached;

        var snapshot = Contexts.Value;
        for (var index = 0; index < snapshot.Length; index++)
        {
            if (snapshot[index].GetTypeInfo(type) is not { } resolved)
                continue;
            _ = InfoByType.TryAdd(type, resolved);
            return resolved;
        }

        throw new InvalidOperationException(missingMessage);
    }

    private static JsonTypeInfo<T> ThrowMissingMetadata<T>() => throw new InvalidOperationException(
        $"No JSON metadata registered for {typeof(T)}. Register a source-generated JsonSerializerContext covering this type (AOT requirement).");

    /// <summary>Swappable registration snapshot. The holder reference is readonly; the array reference stays volatile so readers observe swaps with a single acquire.</summary>
    private sealed class ContextsSnapshot
    {
        private volatile JsonSerializerContext[] _value;

        internal ContextsSnapshot(JsonSerializerContext[] value)
        {
            _value = value;
        }

        internal JsonSerializerContext[] Value
        {
            get => _value;
            set => _value = value;
        }
    }
}
