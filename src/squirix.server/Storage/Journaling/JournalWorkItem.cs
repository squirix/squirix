using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Journaling;

[Immutable]
internal sealed record JournalWorkItem
{
    private JournalWorkItem(JournalWorkKind kind, DurabilityAck? ack, byte[]? frameBytes, int frameLength, int resetSegmentIndex, ulong resetSequence)
    {
        Kind = kind;
        Ack = ack;
        FrameBytes = frameBytes;
        FrameLength = frameLength;
        ResetSegmentIndex = resetSegmentIndex;
        ResetSequence = resetSequence;
    }

    /// <summary>Gets the ack resolved by the journal thread when this item's work completes or fails.</summary>
    internal DurabilityAck? Ack { get; }

    /// <summary>Gets the encoded journal frame bytes owned until the writing completes.</summary>
    internal byte[]? FrameBytes { get; }

    /// <summary>Gets the exact length of the framed payload inside <see cref="FrameBytes" />.</summary>
    internal int FrameLength { get; }

    /// <summary>Gets the kind of journal work to perform on the journal thread.</summary>
    internal JournalWorkKind Kind { get; }

    /// <summary>Gets the active segment index to install during maintenance end.</summary>
    internal int ResetSegmentIndex { get; }

    /// <summary>Gets the next sequence number to install during maintenance end.</summary>
    internal ulong ResetSequence { get; }

    /// <summary>Creates an append item whose frame is staged into the write batch by the journal thread.</summary>
    /// <param name="frameBytes">Encoded frame buffer rented from <c language="csharp">ArrayPool&lt;byte&gt;</c>.</param>
    /// <param name="frameLength">Exact length of the framed payload inside <paramref name="frameBytes" />.</param>
    /// <param name="ack">Optional ack resolving group-commit append completion.</param>
    /// <returns>A new append work item for the journal ring.</returns>
    internal static JournalWorkItem Append(byte[] frameBytes, int frameLength, DurabilityAck? ack = null) => new(JournalWorkKind.Append, ack, frameBytes, frameLength, 0, 0UL);

    /// <summary>Creates an append item whose ack resolves after the frame is written and fsynced.</summary>
    /// <param name="ack">Ack resolved once this item's frame reaches the segment file. Required.</param>
    /// <param name="frameBytes">Encoded frame buffer rented from <c language="csharp">ArrayPool&lt;byte&gt;</c>.</param>
    /// <param name="frameLength">Exact length of the framed payload inside <paramref name="frameBytes" />.</param>
    /// <returns>A new durable append work item for the journal ring.</returns>
    internal static JournalWorkItem AppendWithDurability(DurabilityAck ack, byte[] frameBytes, int frameLength)
    {
        ArgumentNullException.ThrowIfNull(ack);
        return new JournalWorkItem(JournalWorkKind.AppendWithDurability, ack, frameBytes, frameLength, 0, 0UL);
    }

    /// <summary>
    /// Creates a durability checkpoint item carrying <paramref name="ack" />. The ack rides the item's
    /// ring position, so the flush performed while processing this item covers every frame enqueued
    /// before it; only this ack is resolved when processing completes.
    /// </summary>
    /// <param name="ack">Ack resolved when this checkpoint has been processed. Required.</param>
    /// <returns>A new durability checkpoint work item for the journal ring.</returns>
    internal static JournalWorkItem DurabilityCheckpoint(DurabilityAck ack)
    {
        ArgumentNullException.ThrowIfNull(ack);
        return new JournalWorkItem(JournalWorkKind.DurabilityCheckpoint, ack, null, 0, 0, 0UL);
    }

    /// <summary>Creates a maintenance begin item that flushes staged frames and releases the segment writer.</summary>
    /// <param name="ack">Ack resolved after the journal thread flushed and released the segment.</param>
    /// <returns>A new maintenance begin work item for the journal ring.</returns>
    internal static JournalWorkItem MaintenanceBegin(DurabilityAck ack)
    {
        ArgumentNullException.ThrowIfNull(ack);
        return new JournalWorkItem(JournalWorkKind.MaintenanceBegin, ack, null, 0, 0, 0UL);
    }

    /// <summary>Creates a maintenance end item that resyncs in-memory counters from the rewritten segment layout.</summary>
    /// <param name="ack">Ack resolved after the journal thread installed the reset state.</param>
    /// <param name="resetSegmentIndex">Active segment index reported by the compaction rewrite.</param>
    /// <param name="resetSequence">Next journal sequence consistent with the rewritten segments.</param>
    /// <returns>A new maintenance end work item for the journal ring.</returns>
    internal static JournalWorkItem MaintenanceEnd(DurabilityAck ack, int resetSegmentIndex, ulong resetSequence)
    {
        ArgumentNullException.ThrowIfNull(ack);
        return new JournalWorkItem(JournalWorkKind.MaintenanceEnd, ack, null, 0, resetSegmentIndex, resetSequence);
    }

    /// <summary>Creates a shutdown item that flushes staged frames and stops the journal thread.</summary>
    /// <returns>A new shutdown work item for the journal ring.</returns>
    internal static JournalWorkItem Shutdown() => new(JournalWorkKind.Shutdown, null, null, 0, 0, 0UL);
}
