namespace Squirix.Server.Storage.Journaling;

internal readonly struct JournalWorkItem
{
    public JournalWorkKind Kind { get; init; }

    public byte[]? FrameBytes { get; init; }

    public int FrameLength { get; init; }

    public JournalDurabilityWaiter? DurabilityWaiter { get; init; }

    public JournalDurabilityWaiter? Completion { get; init; }

    public int ResetSegmentIndex { get; init; }

    public ulong ResetSequence { get; init; }

    public Manifest.ManifestState? Manifest { get; init; }

    public int RollCurrentJournal { get; init; }

    public ulong RollNextSequence { get; init; }
}
