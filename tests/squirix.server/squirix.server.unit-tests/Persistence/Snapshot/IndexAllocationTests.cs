using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Snapshot;

/// <summary>Verifies the snapshot writer allocates an index above every snapshot file on disk.</summary>
[Immutable]
public sealed class IndexAllocationTests : IsolatedStorageTestBase
{
    /// <summary>An orphan snapshot above the published index moves the next index past it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AllocateIndexSkipsOrphanSnapshot(CancellationToken cancellationToken)
    {
        var writer = new SnapshotWriter(Dir);
        _ = await Assert.That(writer.AllocateIndex(0)).IsEqualTo(1);

        await File.WriteAllBytesAsync(Path.Join(Dir, $"{FilePrefixes.Snapshot}000005{FileExtensions.Snapshot}"), [1], cancellationToken);

        _ = await Assert.That(writer.AllocateIndex(2)).IsEqualTo(6);
        _ = await Assert.That(writer.AllocateIndex(7)).IsEqualTo(8);
    }
}
