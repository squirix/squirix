using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>Coverage for shared <c>man-current</c> pointer reads used after abrupt shutdown.</summary>
public sealed class PointerFileTests : ServerUnitTestBase, IAsyncLifetime
{
    private TempDirectory? _dir;

    /// <summary>Shared-mode pointer reads succeed while a writer-compatible handle remains open.</summary>
    [Fact]
    public async Task SharedReadSucceedsWithOpenWriterHandle()
    {
        var path = Path.Combine(_dir!, "man-current");
#pragma warning disable ZA0301
        var pointerBytes = new byte[Pointer.Size];
#pragma warning restore ZA0301
        Pointer.Write(pointerBytes, 7);
        await File.WriteAllBytesAsync(path, pointerBytes, DefaultCancellationToken);

        await using var writer = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, PointerFile.CompatibleShare, 64, FileOptions.Asynchronous);

        var bytes = await PointerFile.ReadAllBytesAsync(path, DefaultCancellationToken);
        Assert.Equal(7, Pointer.Read(bytes));
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _dir?.Dispose();
        _dir = null;
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask InitializeAsync()
    {
        _dir = new TempDirectory("squirix-pointer-file");
        return ValueTask.CompletedTask;
    }
}
