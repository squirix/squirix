using System;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Squirix.Server.Storage.Snapshot;

internal sealed class TriggerOptionsJsonConverter : JsonConverter<TriggerOptions>
{
    public override TriggerOptions? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Null)
            return null;

        using var document = JsonDocument.ParseValue(ref reader);
        var root = document.RootElement;
        return root.ValueKind != JsonValueKind.Object ? throw new JsonException($"Unexpected token {reader.TokenType} when reading TriggerOptions.") : CreateTriggerOptions(root);
    }

    public override void Write(Utf8JsonWriter writer, TriggerOptions value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteNumber("journalGrowthThrottleBytes", value.JournalGrowthThrottleBytes);
        writer.WriteNumber("latencySloMilliseconds", value.LatencySloMilliseconds);
        writer.WriteString("latencyThrottleDuration", value.LatencyThrottleDuration.ToString("c", CultureInfo.InvariantCulture));
        writer.WriteString("minGapBetweenSnapshots", value.MinGapBetweenSnapshots.ToString("c", CultureInfo.InvariantCulture));
        writer.WriteNumber("snapshotEveryNBytes", value.SnapshotEveryNBytes);
        writer.WriteNumber("snapshotEveryNOps", value.SnapshotEveryNOps);
        writer.WriteString("snapshotInterval", value.SnapshotInterval.ToString("c", CultureInfo.InvariantCulture));
        writer.WriteEndObject();
    }

    private static TriggerOptions CreateTriggerOptions(JsonElement root) => new()
    {
        JournalGrowthThrottleBytes = ReadInt64(root, "journalGrowthThrottleBytes", 0L),
        LatencySloMilliseconds = ReadDouble(root, "latencySloMilliseconds", 0d),
        LatencyThrottleDuration = ReadTimeSpan(root, "latencyThrottleDuration", TimeSpan.FromSeconds(10)),
        MinGapBetweenSnapshots = ReadTimeSpan(root, "minGapBetweenSnapshots", TimeSpan.FromMinutes(1)),
        SnapshotEveryNBytes = ReadInt64(root, "snapshotEveryNBytes", 128L * 1024 * 1024),
        SnapshotEveryNOps = ReadInt64(root, "snapshotEveryNOps", 250_000L),
        SnapshotInterval = ReadTimeSpan(root, "snapshotInterval", TimeSpan.FromMinutes(5)),
    };

    private static double ReadDouble(JsonElement root, string name, double defaultValue)
    {
        return !TryFindProperty(root, name, out var e) ? defaultValue : e.ValueKind switch
        {
            JsonValueKind.Number when e.TryGetDouble(out var number) => number,
            JsonValueKind.String when double.TryParse(e.GetString(), NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var parsed) => parsed,
            _ => throw new JsonException($"Cannot convert {e.ValueKind} to Double for TriggerOptions."),
        };
    }

    private static long ReadInt64(JsonElement root, string name, long defaultValue)
    {
        return !TryFindProperty(root, name, out var element) ? defaultValue : element.ValueKind switch
        {
            JsonValueKind.Number when element.TryGetInt64(out var number) => number,
            JsonValueKind.String when long.TryParse(element.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) => parsed,
            _ => throw new JsonException($"Cannot convert {element.ValueKind} to Int64 for TriggerOptions."),
        };
    }

    private static TimeSpan ReadTimeSpan(JsonElement root, string name, TimeSpan defaultValue)
    {
        if (!TryFindProperty(root, name, out var element))
            return defaultValue;

        if (element.ValueKind == JsonValueKind.String)
        {
            try
            {
                return TimeSpan.Parse(element.GetString() ?? string.Empty, CultureInfo.InvariantCulture);
            }
            catch (FormatException exception)
            {
                throw new JsonException($"Cannot convert {element.ValueKind} to TimeSpan for TriggerOptions.", exception);
            }
            catch (OverflowException exception)
            {
                throw new JsonException($"Cannot convert {element.ValueKind} to TimeSpan for TriggerOptions.", exception);
            }
        }

        throw new JsonException($"Cannot convert {element.ValueKind} to TimeSpan for TriggerOptions.");
    }

    private static bool TryFindProperty(JsonElement root, string name, out JsonElement value)
    {
        if (root.TryGetProperty(name, out value))
            return true;

        foreach (var property in root.EnumerateObject())
        {
            if (!string.Equals(property.Name, name, StringComparison.OrdinalIgnoreCase))
                continue;
            value = property.Value;
            return true;
        }

        value = default;
        return false;
    }
}
