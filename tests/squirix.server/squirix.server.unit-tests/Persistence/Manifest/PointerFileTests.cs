using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>Coverage for shared <c language="csharp">man-current</c> pointer reads used after abrupt shutdown.</summary>
[Immutable]
public sealed class PointerFileTests : IsolatedStorageTestBase
{
    /// <summary>Shared-mode pointer reads succeed while the writer holds its persistent handle.</summary>
    [Test]
    public async Task SharedReadSucceedsWithOpenWriterHandle()
    {
        var path = Path.Join(Dir, "man-current");
        using var writer = new PersistentPointerWriter(path);
        writer.Publish(7);

        _ = await Assert.That(PointerFile.Read(path).ManifestIndex).IsEqualTo(7);
    }

    /// <summary>A missing pointer reads as never published.</summary>
    [Test]
    public async Task MissingPointerReadsAsNeverPublished()
    {
        var selection = PointerFile.Read(Path.Join(Dir, "man-current"));

        _ = await Assert.That(selection.ManifestIndex).IsEqualTo(0);
    }
}
