using System;
using System.Text.Json.Serialization;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Options for the journal metrics exporter background service.</summary>
internal sealed class JournalMetricsExporterOptions
{
    [JsonConstructor]
    internal JournalMetricsExporterOptions()
    {
        Interval = TimeSpan.FromSeconds(5);
    }

    [JsonInclude]
    [JsonPropertyName("interval")]
    internal TimeSpan Interval
    {
        get;
        set
        {
            value.ThrowIfNegativeOrZero(nameof(value), "Interval must be greater than zero.");

            field = value;
        }
    }
}
