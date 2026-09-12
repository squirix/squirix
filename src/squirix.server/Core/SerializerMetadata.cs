using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using System.Threading;

namespace Squirix.Server.Core;

/// <summary>Resolves AOT-safe JSON metadata from registered source-generation contexts.</summary>
/// <remarks>
/// The chain always contains <see cref="SquirixServerHostingJsonContext" />; hosts and tests
/// add their own contexts via <see cref="RegisterContext" /> so closed payload types resolve
/// without reflection. Serialization resolves by runtime type first, so base/interface-declared
/// entries keep derived properties; deserialization resolves by declared type.
/// </remarks>
internal static class SerializerMetadata
{
    private static readonly Lock Sync = new();
    private static volatile List<JsonSerializerContext> _contexts = [SquirixServerHostingJsonContext.Default];

    /// <summary>Adds a source-generation context to the resolution chain.</summary>
    /// <param name="context">Context to consult after the already registered ones.</param>
    internal static void RegisterContext(JsonSerializerContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        lock (Sync)
        {
            if (_contexts.Contains(context))
                return;
            _contexts = [.. _contexts, context];
        }
    }

    /// <summary>Finds the first context providing metadata for <paramref name="type" />.</summary>
    /// <param name="type">Type to look up.</param>
    /// <returns>Owning context, or <see langword="null" /> when no context covers the type.</returns>
    internal static JsonSerializerContext? FindContext(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        var snapshot = _contexts;
        for (var index = 0; index < snapshot.Count; index++)
        {
            var candidate = snapshot[index];
            if (candidate.GetTypeInfo(type) != null)
                return candidate;
        }

        return null;
    }

    /// <summary>Resolves metadata for <typeparamref name="T" /> or throws.</summary>
    /// <typeparam name="T">Declared type to resolve.</typeparam>
    /// <returns>Source-generated metadata for the type.</returns>
    /// <exception cref="InvalidOperationException">Thrown when no registered context covers the type.</exception>
    internal static JsonTypeInfo<T> Resolve<T>()
    {
        var context = FindContext(typeof(T));
        return context?.GetTypeInfo(typeof(T)) is JsonTypeInfo<T> typed ? typed : ThrowMissingMetadata<T>();
    }

    /// <summary>Serializes a value using runtime-type metadata when it differs from the declared type.</summary>
    /// <param name="destination">Stream receiving the payload.</param>
    /// <param name="value">Value to serialize.</param>
    /// <param name="declaredType">Declared compile-time type.</param>
    internal static void Serialize(Stream destination, object? value, Type declaredType)
    {
        var context = ResolveContext(value, declaredType, out var effectiveType);

        // Sync by IServerSerializer contract: callers pass already-buffered destinations,
        // an async state machine per journal write would be pure overhead.
#pragma warning disable MA0045
        JsonSerializer.Serialize(destination, value, effectiveType, context);
#pragma warning restore MA0045
    }

    /// <summary>Serializes a value to a <see cref="JsonElement" /> using runtime-type metadata when it differs.</summary>
    /// <param name="value">Value to serialize.</param>
    /// <param name="declaredType">Declared compile-time type.</param>
    /// <returns>Serialized element.</returns>
    internal static JsonElement SerializeToElement(object? value, Type declaredType)
    {
        var context = ResolveContext(value, declaredType, out var effectiveType);
        return JsonSerializer.SerializeToElement(value, effectiveType, context);
    }

    /// <summary>Serializes a value to UTF-8 bytes using runtime-type metadata when it differs.</summary>
    /// <param name="value">Value to serialize.</param>
    /// <param name="declaredType">Declared compile-time type.</param>
    /// <returns>UTF-8 payload.</returns>
    internal static byte[] SerializeToUtf8Bytes(object? value, Type declaredType)
    {
        var context = ResolveContext(value, declaredType, out var effectiveType);
        return JsonSerializer.SerializeToUtf8Bytes(value, effectiveType, context);
    }

    private static JsonSerializerContext ResolveContext(object? value, Type declaredType, out Type effectiveType)
    {
        var runtimeType = value?.GetType();
        if (runtimeType != null && runtimeType != declaredType)
        {
            effectiveType = runtimeType;
            return FindContext(runtimeType) ?? ThrowMissingMetadata($"No JSON metadata registered for {runtimeType} (runtime type of {declaredType}). Register a source-generated JsonSerializerContext covering this type (AOT requirement).");
        }

        effectiveType = declaredType;
        return FindContext(declaredType) ?? ThrowMissingMetadata($"No JSON metadata registered for {declaredType}. Register a source-generated JsonSerializerContext covering this type (AOT requirement).");
    }

    private static JsonSerializerContext ThrowMissingMetadata(string message) => throw new InvalidOperationException(message);

    private static JsonTypeInfo<T> ThrowMissingMetadata<T>() => throw new InvalidOperationException(
        $"No JSON metadata registered for {typeof(T)}. Register a source-generated JsonSerializerContext covering this type (AOT requirement).");
}
