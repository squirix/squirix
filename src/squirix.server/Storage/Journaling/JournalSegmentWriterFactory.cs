using Squirix.Server.Storage.Journaling.Platform;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Creates <see cref="IJournalSegmentWriter"/> instances for Pipelined.</summary>
internal static class JournalSegmentWriterFactory
{
    public static IJournalSegmentWriter Create(JournalPlatformBackend backend)
    {
        // PERF note (Linux): segment writes and fsync can be made faster with io_uring batching - stage
        // the submission entries and issue one ring-enter per group commit instead of one flush per op.
        // That raw ring was archived after it crashed with a fatal access violation on Linux; the code
        // now lives in the private repo alexander-efremov/squirix-linux-iouring. Reintroduce it from
        // there once it is proven safe. For now every backend uses the memory-safe RandomAccess writer.
        return backend switch
        {
            _ => new RandomAccessJournalSegmentWriter(),
        };
    }
}
