using System;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Snapshot;

/// <summary>
/// Configuration for time/volume based snapshot triggers and throttling guards driven by journal growth
/// and latency SLOs. All thresholds are evaluated cooperatively: a snapshot is eligible when at least
/// one trigger is satisfied and no active throttling guard is engaged.
/// </summary>
/// <remarks>
///     <para>
///     Typical triggering conditions:
///     <list type="bullet">
///         <item>
///             <description><see cref="SnapshotInterval" /> elapsed since the last snapshot.</description>
///         </item>
///         <item>
///             <description><see cref="SnapshotEveryNOps" /> operations have been applied since the last snapshot.</description>
///         </item>
///         <item>
///             <description><see cref="SnapshotEveryNBytes" /> of journal have been appended since the last snapshot.</description>
///         </item>
///     </list>
///     </para>
///     <para>
///     Throttling guards may suppress otherwise-eligible snapshots:
///     <list type="bullet">
///         <item>
///             <description><see cref="JournalGrowthThrottleBytes" /> requires a minimum journal delta before allowing a snapshot.</description>
///         </item>
///         <item>
///             <description>
///             Latency SLO breaches (<see cref="LatencySloMilliseconds" />) suppress snapshots for
///             <see cref="LatencyThrottleDuration" />.
///             </description>
///         </item>
///     </list>
///     </para>
/// </remarks>
[JsonConverter(typeof(TriggerOptionsJsonConverter))]
internal sealed class TriggerOptions
{
    internal TriggerOptions()
    {
        JournalGrowthThrottleBytes = 0L;
        LatencySloMilliseconds = 0d;
        LatencyThrottleDuration = TimeSpan.FromSeconds(10);
        MinGapBetweenSnapshots = TimeSpan.FromMinutes(1);
        SnapshotEveryNBytes = 128L * 1024 * 1024;
        SnapshotEveryNOps = 250_000L;
        SnapshotInterval = TimeSpan.FromMinutes(5);
    }

    /// <summary>
    /// Gets the minimum journal byte delta required before a snapshot is allowed, even when other triggers are satisfied.
    /// The default is 0 (disabled).
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is negative.</exception>
    [JsonInclude]
    internal long JournalGrowthThrottleBytes
    {
        get;
        init
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, "JournalGrowthThrottleBytes cannot be negative.");

            field = value;
        }
    }

    /// <summary>
    /// Gets the latency SLO for journal append operations, in milliseconds.
    /// If the observed p95 (or chosen percentile) exceeds this value within the evaluation window,
    /// snapshot attempts are throttled for <see cref="LatencyThrottleDuration" />. Default is 0 (disabled).
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is negative, NaN, or infinite.</exception>
    [JsonInclude]
    internal double LatencySloMilliseconds
    {
        get;
        init
        {
            if (value < 0 || double.IsNaN(value) || double.IsInfinity(value))
                throw new ArgumentOutOfRangeException(nameof(value), value, "LatencySloMilliseconds must be a finite non-negative value.");

            field = value;
        }
    }

    /// <summary>
    /// Gets the duration to suppress snapshot attempts after a latency SLO breach.
    /// Default is 10 seconds.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is negative.</exception>
    [JsonInclude]
    internal TimeSpan LatencyThrottleDuration
    {
        get;
        init
        {
            value.ThrowIfNegative(nameof(value), "LatencyThrottleDuration cannot be negative.");
            field = value;
        }
    }

    /// <summary>
    /// Gets the debounce guard: a minimum gap enforced between consecutive snapshots even if triggers fire back-to-back.
    /// Default is 1 minute.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is negative.</exception>
    [JsonInclude]
    internal TimeSpan MinGapBetweenSnapshots
    {
        get;
        init
        {
            value.ThrowIfNegative(nameof(value), "MinGapBetweenSnapshots cannot be negative.");
            field = value;
        }
    }

    /// <summary>
    /// Gets the journal-size trigger: snapshot becomes eligible after at least this many bytes have been appended to the journal
    /// since the previous snapshot. Default is 128 MiB.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is negative.</exception>
    [JsonInclude]
    internal long SnapshotEveryNBytes
    {
        get;
        init
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, "SnapshotEveryNBytes cannot be negative.");

            field = value;
        }
    }

    /// <summary>
    /// Gets the operation-count trigger: snapshot becomes eligible after at least this many mutating operations
    /// have been applied since the previous snapshot. Default is 250,000.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is negative.</exception>
    [JsonInclude]
    internal long SnapshotEveryNOps
    {
        get;
        init
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException(nameof(value), value, "SnapshotEveryNOps cannot be negative.");

            field = value;
        }
    }

    /// <summary>
    /// Gets the time-based trigger interval: minimum elapsed time since the previous snapshot to consider a new snapshot.
    /// Default is 5 minutes.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is not positive.</exception>
    [JsonInclude]
    internal TimeSpan SnapshotInterval
    {
        get;
        init
        {
            value.ThrowIfNegativeOrZero(nameof(value), "SnapshotInterval must be greater than zero.");
            field = value;
        }
    }

    /// <summary>Source-generated-friendly JSON converter preserving partial-payload semantics.</summary>
    /// <remarks>
    /// The source generator assigns <see langword="default" /> to absent <c language="csharp">init</c> properties, which trips
    /// validating setters. This converter sets only properties present in the payload, matching
    /// the historical reflection behavior for partial option documents.
    /// </remarks>
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
}
