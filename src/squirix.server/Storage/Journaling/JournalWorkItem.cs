namespace Squirix.Server.Storage.Journaling;

internal sealed record JournalWorkItem
{
    internal JournalWorkItem(
        JournalWorkKind kind,
        JournalDurabilityWaiter? completion = null,
        JournalDurabilityWaiter? durabilityWaiter = null,
        byte[]? frameBytes = null,
        int frameLength = 0,
        int resetSegmentIndex = 0,
        ulong resetSequence = 0)
    {
        Kind = kind;
        Completion = completion;
        DurabilityWaiter = durabilityWaiter;
        FrameBytes = frameBytes;
        FrameLength = frameLength;
        ResetSegmentIndex = resetSegmentIndex;
        ResetSequence = resetSequence;
    }

    internal JournalDurabilityWaiter? Completion { get; }

    internal JournalDurabilityWaiter? DurabilityWaiter { get; }

    internal byte[]? FrameBytes { get; }

    internal int FrameLength { get; }

    internal JournalWorkKind Kind { get; }

    internal int ResetSegmentIndex { get; }

    internal ulong ResetSequence { get; }
}
