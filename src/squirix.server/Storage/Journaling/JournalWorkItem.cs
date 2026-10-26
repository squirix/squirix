using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Journaling;

[Immutable]
internal sealed record JournalWorkItem
{
    private JournalWorkItem(JournalWorkKind kind, TaskCompletionSource? ack, byte[]? frameBytes, int frameLength)
    {
        Kind = kind;
        Ack = ack;
        FrameBytes = frameBytes;
        FrameLength = frameLength;
    }

    /// <summary>Gets the ack resolved by the journal thread when this item's work completes or fails.</summary>
    internal TaskCompletionSource? Ack { get; }

    /// <summary>Gets the encoded journal frame bytes owned until the writing completes.</summary>
    internal byte[]? FrameBytes { get; }

    /// <summary>Gets the exact length of the framed payload inside <see cref="FrameBytes" />.</summary>
    internal int FrameLength { get; }

    /// <summary>Gets the kind of journal work to perform on the journal thread.</summary>
    internal JournalWorkKind Kind { get; }

    /// <summary>Creates an append item whose frame is staged into the write batch by the journal thread.</summary>
    /// <param name="frameBytes">Encoded frame buffer rented from <c language="csharp">ArrayPool&lt;byte&gt;</c>.</param>
    /// <param name="frameLength">Exact length of the framed payload inside <paramref name="frameBytes" />.</param>
    /// <param name="ack">
    /// Optional ack resolving group-commit append completion. A plain append carries none: append admission already refused any frame
    /// the journal thread could reject for capacity, and the journal thread fails the pipeline if it ever rejects an append without an ack.
    /// </param>
    /// <returns>A new append work item for the journal ring.</returns>
    internal static JournalWorkItem Append(byte[] frameBytes, int frameLength, TaskCompletionSource? ack = null) => new(JournalWorkKind.Append, ack, frameBytes, frameLength);

    /// <summary>
    /// Creates a durability checkpoint item carrying <paramref name="ack" />. The ack rides the item's
    /// ring position, so the flush performed while processing this item covers every frame enqueued
    /// before it; only this ack is resolved when processing completes.
    /// </summary>
    /// <param name="ack">Ack resolved when this checkpoint has been processed. Required.</param>
    /// <returns>A new durability checkpoint work item for the journal ring.</returns>
    internal static JournalWorkItem DurabilityCheckpoint(TaskCompletionSource ack)
    {
        ArgumentNullException.ThrowIfNull(ack);
        return new JournalWorkItem(JournalWorkKind.DurabilityCheckpoint, ack, null, 0);
    }

    /// <summary>Creates a shutdown item that flushes staged frames and stops the journal thread.</summary>
    /// <returns>A new shutdown work item for the journal ring.</returns>
    internal static JournalWorkItem Shutdown() => new(JournalWorkKind.Shutdown, null, null, 0);
}
