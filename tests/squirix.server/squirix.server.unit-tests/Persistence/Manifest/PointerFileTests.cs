using System;
using System.IO;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>Coverage for shared <c>man-current</c> pointer reads used after abrupt shutdown.</summary>
[Immutable]
public sealed class PointerFileTests : ServerUnitTestBase
{
    /// <summary>Shared-mode pointer reads succeed while a writer-compatible handle remains open.</summary>
    [Fact]
    public void SharedReadSucceedsWithOpenWriterHandle()
    {
        using var dir = new TempDirectory("squirix-pointer-file");
        var path = Path.Join(dir, "man-current");
        Span<byte> pointer = stackalloc byte[Pointer.Size];
        Pointer.Write(pointer, 7);
        using (var create = File.OpenHandle(path, FileMode.Create, FileAccess.Write, FileShare.None))
            RandomAccess.Write(create, pointer, 0);

        using var writer = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, PointerFile.CompatibleShare, 64, FileOptions.None);
        Assert.Equal(7, PointerFile.ReadIndex(path));
    }
}
