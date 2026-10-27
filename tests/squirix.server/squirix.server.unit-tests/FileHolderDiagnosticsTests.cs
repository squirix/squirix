using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Tests for <see cref="FileHolderDiagnostics" />.</summary>
[Immutable]
public sealed class FileHolderDiagnosticsTests : ServerUnitTestBase
{
    /// <summary>A file held open by this process names this process.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HeldFileNamesThisProcess(CancellationToken cancellationToken)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var dir = new TempDirectory("squirix-holders-held");
        var path = Path.Join(dir, "held.bin");
        await File.WriteAllBytesAsync(path, ReadOnlyMemory<byte>.Of(1), cancellationToken);
        using var holder = File.OpenHandle(path);

        var description = FileHolderDiagnostics.DescribeHolders(path);

        _ = await Assert.That(description).Contains($"pid {Environment.ProcessId}");
    }

    /// <summary>A missing path has no holder description and does not throw.</summary>
    [Test]
    public async Task MissingPathHasNoHolders()
    {
        using var dir = new TempDirectory("squirix-holders-missing");

        _ = await Assert.That(FileHolderDiagnostics.DescribeHolders(Path.Join(dir, "missing.bin"))).IsNull();
    }

    /// <summary>A file nobody holds has no holder description.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnheldFileHasNoHolders(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-holders-free");
        var path = Path.Join(dir, "free.bin");
        await File.WriteAllBytesAsync(path, ReadOnlyMemory<byte>.Of(1), cancellationToken);

        var description = FileHolderDiagnostics.DescribeHolders(path);

        // An on-close scanner may hold the new file briefly, but this process never does.
        _ = await Assert.That(description?.Contains($"pid {Environment.ProcessId}", StringComparison.Ordinal) != true).IsTrue();
    }
}
