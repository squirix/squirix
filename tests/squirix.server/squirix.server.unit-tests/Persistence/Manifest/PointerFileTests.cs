using System;
using System.IO;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>Coverage for shared <c>man-current</c> pointer reads used after abrupt shutdown.</summary>
[Immutable]
public sealed class PointerFileTests : IsolatedStorageTestBase
{
    /// <summary>Shared-mode pointer reads succeed while a writer-compatible handle remains open.</summary>
    [Fact]
    public void SharedReadSucceedsWithOpenWriterHandle()
    {
        var path = Path.Join(Dir, "man-current");
        Span<byte> pointer = stackalloc byte[Pointer.Size];
        Pointer.Write(pointer, 7);
        using (var create = File.OpenHandle(path, FileMode.Create, FileAccess.Write, FileShare.None))
            RandomAccess.Write(create, pointer, 0);

        using var writer = File.OpenHandle(path, FileMode.Open, FileAccess.ReadWrite, PointerFile.CompatibleShare);
        Assert.Equal(7, PointerFile.ReadIndex(path));
    }
}
