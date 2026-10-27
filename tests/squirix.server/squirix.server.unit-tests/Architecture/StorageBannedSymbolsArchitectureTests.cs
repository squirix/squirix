using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.SourceScanning;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>
/// Enforces the storage-only bans for <c language="csharp">src/squirix.server/Storage</c>; the scanning rules and their known limits
/// are described on <see cref="StorageBanScanner"/>.
/// </summary>
[Immutable]
public sealed class StorageBannedSymbolsArchitectureTests : ServerUnitTestBase
{
    private const string SanctionedReturnHelper = "ArrayPoolExtensions.cs";

    /// <summary>Ensures persistence sources do not use the symbols banned for storage code.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StorageSourcesDoNotUseBannedSymbols(CancellationToken cancellationToken)
    {
        var files = await ServerSourceFiles.EnumerateCsharpFilesAsync("Storage");
        var violations = new List<string>();
        for (var index = 0; index < files.Count; index++)
        {
            var path = files[index];
            if (string.Equals(Path.GetFileName(path), SanctionedReturnHelper, StringComparison.Ordinal))
                continue;

            StorageBanScanner.FindViolations(path, await File.ReadAllTextAsync(path, cancellationToken), violations);
        }

        _ = await Assert.That(violations).IsEmpty().Because(string.Join(Environment.NewLine, violations));
    }
}
