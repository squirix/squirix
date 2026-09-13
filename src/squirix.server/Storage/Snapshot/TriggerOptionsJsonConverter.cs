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
        if (reader.TokenType != JsonTokenType.StartObject)
            throw new JsonException($"Unexpected token {reader.TokenType} when reading TriggerOptions.");

        var state = new TriggerOptionsState
        {
            LatencyThrottleDuration = TimeSpan.FromSeconds(10),
            MinGapBetweenSnapshots = TimeSpan.FromMinutes(1),
            SnapshotEveryNBytes = 128L * 1024 * 1024,
            SnapshotEveryNOps = 250_000L,
            SnapshotInterval = TimeSpan.FromMinutes(5),
        };

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
                return state.Build();

            if (reader.TokenType != JsonTokenType.PropertyName)
                throw new JsonException($"Unexpected token {reader.TokenType} when reading TriggerOptions.");

            var name = reader.GetString();
            if (!reader.Read())
                throw new JsonException("Unexpected end of TriggerOptions payload.");

            ReadProperty(ref reader, name, state);
        }

        throw new JsonException("Unexpected end of TriggerOptions payload.");
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

    private static TimeSpan ReadTimeSpan(ref Utf8JsonReader reader)
    {
        try
        {
            return TimeSpan.Parse(reader.GetString() ?? string.Empty, CultureInfo.InvariantCulture);
        }
        catch (InvalidOperationException exception)
        {
            throw new JsonException($"Cannot convert {reader.TokenType} to TimeSpan for TriggerOptions.", exception);
        }
        catch (FormatException exception)
        {
            throw new JsonException($"Cannot convert {reader.TokenType} to TimeSpan for TriggerOptions.", exception);
        }
        catch (OverflowException exception)
        {
            throw new JsonException($"Cannot convert {reader.TokenType} to TimeSpan for TriggerOptions.", exception);
        }
    }

    private static void ReadProperty(ref Utf8JsonReader reader, string? name, TriggerOptionsState state)
    {
        if (string.Equals(name, "journalGrowthThrottleBytes", StringComparison.OrdinalIgnoreCase))
            state.JournalGrowthThrottleBytes = ReadInt64(ref reader);
        else if (string.Equals(name, "latencySloMilliseconds", StringComparison.OrdinalIgnoreCase))
            state.LatencySloMilliseconds = ReadDouble(ref reader);
        else if (string.Equals(name, "latencyThrottleDuration", StringComparison.OrdinalIgnoreCase))
            state.LatencyThrottleDuration = ReadTimeSpan(ref reader);
        else if (string.Equals(name, "minGapBetweenSnapshots", StringComparison.OrdinalIgnoreCase))
            state.MinGapBetweenSnapshots = ReadTimeSpan(ref reader);
        else if (string.Equals(name, "snapshotEveryNBytes", StringComparison.OrdinalIgnoreCase))
            state.SnapshotEveryNBytes = ReadInt64(ref reader);
        else if (string.Equals(name, "snapshotEveryNOps", StringComparison.OrdinalIgnoreCase))
            state.SnapshotEveryNOps = ReadInt64(ref reader);
        else if (string.Equals(name, "snapshotInterval", StringComparison.OrdinalIgnoreCase))
            state.SnapshotInterval = ReadTimeSpan(ref reader);
        else
            reader.Skip();
    }

    private static long ReadInt64(ref Utf8JsonReader reader)
    {
        try
        {
            if (reader.TokenType == JsonTokenType.Number)
                return reader.GetInt64();
            if (reader.TokenType == JsonTokenType.String
                && long.TryParse(reader.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
                return parsed;
        }
        catch (InvalidOperationException exception)
        {
            throw new JsonException($"Cannot convert {reader.TokenType} to Int64 for TriggerOptions.", exception);
        }
        catch (OverflowException exception)
        {
            throw new JsonException($"Cannot convert {reader.TokenType} to Int64 for TriggerOptions.", exception);
        }

        throw new JsonException($"Cannot convert {reader.TokenType} to Int64 for TriggerOptions.");
    }

    private static double ReadDouble(ref Utf8JsonReader reader)
    {
        try
        {
            if (reader.TokenType == JsonTokenType.Number)
                return reader.GetDouble();
            if (reader.TokenType == JsonTokenType.String
                && double.TryParse(
                    reader.GetString(),
                    NumberStyles.Float | NumberStyles.AllowThousands,
                    CultureInfo.InvariantCulture,
                    out var parsed))
                return parsed;
        }
        catch (InvalidOperationException exception)
        {
            throw new JsonException($"Cannot convert {reader.TokenType} to Double for TriggerOptions.", exception);
        }
        catch (OverflowException exception)
        {
            throw new JsonException($"Cannot convert {reader.TokenType} to Double for TriggerOptions.", exception);
        }

        throw new JsonException($"Cannot convert {reader.TokenType} to Double for TriggerOptions.");
    }

    private sealed class TriggerOptionsState
    {
        internal long JournalGrowthThrottleBytes { get; set; }

        internal double LatencySloMilliseconds { get; set; }

        internal TimeSpan LatencyThrottleDuration { get; set; }

        internal TimeSpan MinGapBetweenSnapshots { get; set; }

        internal long SnapshotEveryNBytes { get; set; }

        internal long SnapshotEveryNOps { get; set; }

        internal TimeSpan SnapshotInterval { get; set; }

        internal TriggerOptions Build() => new()
        {
            JournalGrowthThrottleBytes = JournalGrowthThrottleBytes,
            LatencySloMilliseconds = LatencySloMilliseconds,
            LatencyThrottleDuration = LatencyThrottleDuration,
            MinGapBetweenSnapshots = MinGapBetweenSnapshots,
            SnapshotEveryNBytes = SnapshotEveryNBytes,
            SnapshotEveryNOps = SnapshotEveryNOps,
            SnapshotInterval = SnapshotInterval,
        };
    }
}
