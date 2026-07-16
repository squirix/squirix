using System;
using System.Text.Json.Serialization;

namespace Squirix.Server.Node.Services;

/// <summary>Options for the journal metrics exporter background service.</summary>
internal sealed class JournalMetricsExporterOptions
{
    [JsonConstructor]
    public JournalMetricsExporterOptions()
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
            if (value <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(value), value, "Interval must be greater than zero.");

            field = value;
        }
    }
}
