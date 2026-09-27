namespace Squirix.Server.Storage.Journaling;

/// <summary>Exposes the probe that tracks journal I/O and mutation gate holders for stall diagnostics.</summary>
internal interface IJournalStallProbeSource
{
    /// <summary>Gets the stall probe.</summary>
    JournalStallProbe StallProbe { get; }
}
