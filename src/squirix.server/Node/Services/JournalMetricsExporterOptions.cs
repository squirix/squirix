using System;
using System.Diagnostics.CodeAnalysis;

namespace Squirix.Server.Node.Services;

/// <summary>
/// Options for the journal metrics exporter background service.
/// </summary>
internal sealed class JournalMetricsExporterOptions
{
    public JournalMetricsExporterOptions()
    {
        Interval = TimeSpan.FromSeconds(5);
    }

    public TimeSpan Interval
    {
        get;
        [SuppressMessage("ReSharper", "PropertyCanBeMadeInitOnly.Global", Justification = "Used in serialization.")]
        set
        {
            if (value <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(Interval), value, "Interval must be greater than zero.");

            field = value;
        }
    }
}
