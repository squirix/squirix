using System.Threading;

namespace Squirix.Server.Storage.Journaling.Read;

/// <summary>Pattern-based replay over journal segments without <see cref="System.Collections.Generic.IEnumerable{T}" />.</summary>
internal readonly struct JournalReplaySequence
{
    private readonly JournalSegment[] _segments;
    private readonly CancellationToken _cancellationToken;

    internal JournalReplaySequence(string dataDir, int fromSegment, CancellationToken cancellationToken)
    {
        _segments = JournalReadPath.EnumerateSegments(dataDir, fromSegment);
        _cancellationToken = cancellationToken;
    }

    public JournalReplayEnumerator GetEnumerator() => new(_segments, _cancellationToken);
}
