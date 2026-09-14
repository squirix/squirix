namespace Squirix.Server.Storage.Journaling;

/// <summary>Mutable journal event-loop state used for segment rolls.</summary>
internal interface IJournalEventLoopRollState
{
    string? ActiveSegmentPath { get; }

    int CurrentSegmentIndex { get; }

    int JournalSegmentCount { get; }

    int PendingRollTargetSegmentIndex { get; }

    bool SegmentRollInFlight { get; }

    bool TryConsumeSegmentRollCompletion();

    void MarkSegmentRollCompletionPending();

    void IncrementJournalSegmentCount();

    void SetActiveSegmentPath(string? value);

    void SetCurrentSegmentIndex(int value);

    void SetJournalSegmentCount(int value);

    void SetPendingRollTargetSegmentIndex(int value);

    void SetSegmentRollInFlight(bool value);
}
