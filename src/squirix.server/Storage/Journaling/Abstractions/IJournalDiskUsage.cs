namespace Squirix.Server.Storage.Journaling.Abstractions;

/// <summary>Exposes on-disk journal usage against configured capacity for readiness diagnostics.</summary>
internal interface IJournalDiskUsage
{
    /// <summary>Gets the soft high-water mark in bytes (80% of <see cref="MaxBytes" />).</summary>
    long HighWaterBytes { get; }

    /// <summary>Gets the configured journal total byte cap.</summary>
    long MaxBytes { get; }

    /// <summary>Gets the current on-disk journal total bytes (journal-thread counter).</summary>
    long UsedBytes { get; }
}
