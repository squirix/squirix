#pragma warning disable MA0182 // Referenced by journal writer tests as they migrate to shared options helper.

using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.PipelinedWal;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Persistence options for JsonFramed journal unit tests.</summary>
internal static class JsonFramedJournalTestOptions
{
    /// <summary>Builds persistence options with <see cref="JournalBackend.JsonFramed"/> for a test data directory.</summary>
    /// <param name="dataDir">Root data directory for the test run.</param>
    /// <returns>Persistence options configured for JsonFramed journaling.</returns>
    public static PersistenceOptions ForDirectory(string dataDir) => new()
    {
        DataDir = dataDir,
        JournalBackend = JournalBackend.JsonFramed,
    };
}
#pragma warning restore MA0182
