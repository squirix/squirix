using System.Threading;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Creates <see cref="IJournalSegmentReader"/> instances for journal replay.</summary>
internal static class JournalSegmentReaderFactory
{
    public static IJournalSegmentReader Open(string path, bool tolerateTruncatedTail, CancellationToken cancellationToken) =>
        new BinaryJournalSegmentReader(path, tolerateTruncatedTail, cancellationToken);
}
