using System;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.JournalProto;

namespace Squirix.Server.Storage.Maintenance;

/// <summary>
/// Provides read-only journal directory dumps.
/// </summary>
[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal static class JournalDumper
{
    /// <summary>
    /// Loads journal metadata and a bounded frame preview from a storage directory.
    /// </summary>
    /// <param name="dataDirectory">Storage data directory.</param>
    /// <param name="fromSegment">First segment index to read.</param>
    /// <param name="maxFrames">Maximum number of frame previews.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The dump report.</returns>
    public static JournalDumpReport DumpDirectory(string dataDirectory, int fromSegment = 1, int maxFrames = 100, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dataDirectory);
        ArgumentOutOfRangeException.ThrowIfLessThan(fromSegment, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxFrames, 1);

        var frameCount = 0;
        var firstSequence = ulong.MaxValue;
        var lastSequence = 0UL;
        var frames = new List<JournalFramePreview>(maxFrames);
        foreach (var envelope in JournalReader.ReadAll(dataDirectory, fromSegment, cancellationToken))
        {
            frameCount++;
            firstSequence = Math.Min(firstSequence, envelope.Seq);
            lastSequence = Math.Max(lastSequence, envelope.Seq);
            if (frames.Count < maxFrames)
                frames.Add(MapFrame(envelope));
        }

        return new JournalDumpReport(frameCount, frameCount == 0 ? 0UL : firstSequence, lastSequence, frames);
    }

    private static JournalFramePreview MapFrame(JournalEnvelope envelope)
    {
        return envelope.OpCase switch
        {
            JournalEnvelope.OpOneofCase.Put when envelope.Put is { } put => new JournalFramePreview(envelope.Seq, "Put", put.Item?.Key, put.Item?.Namespace, null),
            JournalEnvelope.OpOneofCase.Remove when envelope.Remove is { } remove => new JournalFramePreview(envelope.Seq, "Remove", remove.Key, remove.Namespace, null),
            JournalEnvelope.OpOneofCase.RemoveExpiration when envelope.RemoveExpiration is { } removeExpiration => new JournalFramePreview(
                envelope.Seq,
                "RemoveExpiration",
                removeExpiration.Key,
                removeExpiration.Namespace,
                null),
            JournalEnvelope.OpOneofCase.TouchExpiration when envelope.TouchExpiration is { } touchExpiration => new JournalFramePreview(
                envelope.Seq,
                "TouchExpiration",
                touchExpiration.Key,
                touchExpiration.Namespace,
                null),
            JournalEnvelope.OpOneofCase.None => new JournalFramePreview(envelope.Seq, "None", null, null, null),
            _ => new JournalFramePreview(envelope.Seq, envelope.OpCase.ToString(), null, null, null),
        };
    }
}
