namespace Squirix.Server.Storage.Replication;

/// <summary>Test seam for deterministic crash/fault injection at durability boundaries.</summary>
internal interface IFollowerLogFaultHooks
{
    /// <summary>Invoked after frame bytes are written to the log stream, before they are flushed.</summary>
    void OnFrameWritten();

    /// <summary>Invoked after a durable flush of log or metadata bytes.</summary>
    void OnFlushed();

    /// <summary>Invoked after a durable commit-index advance.</summary>
    void OnCommitAdvanced();

    /// <summary>Invoked before committed entries are returned to a caller (memory-apply boundary).</summary>
    void OnBeforeMemoryApply();
}
