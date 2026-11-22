using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Checks that pruning build directories preserves the source selection and its order.</summary>
[Immutable]
public sealed class ServerSourceFilesTests : IsolatedStorageTestBase
{
    /// <summary>Pruning matches the previous recursive scan and exact path-segment filters.</summary>
    /// <param name="excludeBin">Whether bin directories are excluded.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task PrunedScanPreservesPathsAndOrder(bool excludeBin, CancellationToken cancellationToken)
    {
        string[] names =
        [
            "root.cs", "ignore.txt", "obj/generated.cs", "nested/obj/generated.cs",
            "bin/copied.cs", "nested/bin/copied.cs", "ordinary/keep.cs",
            "object/keep.cs", "case/Obj/keep.cs", ".hidden/keep.cs", "uppercase.CS",
        ];
        foreach (var name in names)
        {
            var path = Path.Join(Dir, name.Replace('/', Path.DirectorySeparatorChar));
            _ = Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            await File.WriteAllTextAsync(path, string.Empty, cancellationToken);
        }

        var separator = Path.DirectorySeparatorChar;
        var expected = new List<string>();
        foreach (var path in Directory.GetFiles(Dir, "*.cs", SearchOption.AllDirectories))
        {
            if (!path.Contains($"{separator}obj{separator}", StringComparison.Ordinal) &&
                (!excludeBin || !path.Contains($"{separator}bin{separator}", StringComparison.Ordinal)))
                expected.Add(path);
        }

        var actual = new List<string>(ServerSourceFiles.EnumerateUnder(Dir, excludeBin));

        _ = await Assert.That(expected.Count).IsGreaterThan(0);
        await SequenceAssert.EqualAsync(expected, actual, StringComparer.Ordinal);
        _ = await Assert.That(ServerSourceFiles.EnumerateUnder(Path.Join(Dir, "obj"), excludeBin)).IsEmpty();
    }
}
