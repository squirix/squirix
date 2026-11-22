using System;
using System.Text.Json;
using Squirix.Server.Core;

namespace Squirix.Server.Node.Hosting;

/// <summary>Reads the optional eviction policy from the selected node settings file.</summary>
internal static class EvictionSettings
{
    internal static EvictionOptions? Read(JsonElement root)
    {
        if (!root.TryGetProperty("CacheEviction", out var section))
            return null;
        if (section.ValueKind != JsonValueKind.Object)
            throw new InvalidOperationException("CacheEviction must be an object.");

        string? mode = null;
        int? maxKeys = null;
        foreach (var property in section.EnumerateObject())
        {
            switch (property.Name)
            {
                case "mode" when mode == null && property.Value.ValueKind == JsonValueKind.String:
                    mode = property.Value.GetString();
                    break;
                case "maxKeys" when maxKeys == null && property.Value.ValueKind == JsonValueKind.Number && property.Value.TryGetInt32(out var count) && count > 0:
                    maxKeys = count;
                    break;
                default:
                    throw new InvalidOperationException($"Invalid or duplicate CacheEviction.{property.Name}. Use mode and, for keyCount, a positive integer maxKeys.");
            }
        }

        return Resolve(mode, maxKeys);
    }

    internal static EvictionOptions? Resolve(string? mode, int? maxKeys) => (mode, maxKeys) switch
    {
        ("disabled", null) => null,
        ("memory", null) => new EvictionOptions { EvictOnMemoryPressure = true },
        ("keyCount", > 0) => new EvictionOptions { Capacity = maxKeys },
        ("keyCount", _) => throw new InvalidOperationException("CacheEviction.maxKeys is required for keyCount mode."),
        ("memory" or "disabled", _) => throw new InvalidOperationException("CacheEviction.maxKeys applies only to keyCount mode."),
        _ => throw new InvalidOperationException("CacheEviction.mode must be disabled, keyCount or memory."),
    };
}
