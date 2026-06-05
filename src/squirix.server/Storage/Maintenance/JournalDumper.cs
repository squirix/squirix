using System;
using System.Collections.Generic;
using System.Threading;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.JournalProto;

namespace Squirix.Server.Storage.Maintenance;

/// <summary>
/// Provides read-only journal directory dumps.
/// </summary>
[JetBrains.Annotations.UsedImplicitly(JetBrains.Annotations.ImplicitUseTargetFlags.WithMembers)]
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
            JournalEnvelope.OpOneofCase.Put => new JournalFramePreview(envelope.Seq, "Put", envelope.Put.Item?.Key, envelope.Put.Item?.Namespace, null),
            JournalEnvelope.OpOneofCase.Remove => new JournalFramePreview(envelope.Seq, "Remove", envelope.Remove.Key, envelope.Remove.Namespace, null),
            JournalEnvelope.OpOneofCase.RemoveExpiration => new JournalFramePreview(envelope.Seq, "RemoveExpiration", envelope.RemoveExpiration.Key, envelope.RemoveExpiration.Namespace, null),
            JournalEnvelope.OpOneofCase.TouchExpiration => new JournalFramePreview(envelope.Seq, "TouchExpiration", envelope.TouchExpiration.Key, envelope.TouchExpiration.Namespace, null),
            JournalEnvelope.OpOneofCase.None => new JournalFramePreview(envelope.Seq, "None", null, null, null),
            _ => new JournalFramePreview(envelope.Seq, envelope.OpCase.ToString(), null, null, null),
        };
    }
}
