using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>Coverage for shared <c language="csharp">man-current</c> reads used after abrupt shutdown.</summary>
[Immutable]
public sealed class SlotFileTests : IsolatedStorageTestBase
{
    /// <summary>Shared-mode reads succeed while the writer holds its persistent handle.</summary>
    [Test]
    public async Task SharedReadSucceedsWithOpenWriterHandle()
    {
        var path = Path.Join(Dir, "man-current");
        using var writer = new SlotWriter(path);
        _ = writer.Publish(new State { CurrentJournal = 7 });

        _ = await Assert.That(SlotFile.Read(path).State.CurrentJournal).IsEqualTo(7);
    }

    /// <summary>A missing file reads as a never published, default manifest.</summary>
    [Test]
    public async Task MissingFileReadsAsNeverPublished()
    {
        var selection = SlotFile.Read(Path.Join(Dir, "man-current"));

        _ = await Assert.That(selection.Sequence).IsEqualTo(0UL);
        _ = await Assert.That(selection.State.CurrentJournal).IsEqualTo(1);
        _ = await Assert.That(selection.State.NextSequence).IsEqualTo(1UL);
        _ = await Assert.That(selection.State.LastSnapshot).IsNull();
    }
}
