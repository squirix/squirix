using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.Support;
using Squirix.Testing;
using Squirix.Transport.Wire.Cache;
using TUnit.Core;

namespace Squirix.Server.UnitTests.ApiSnapshots;

/// <summary>Golden snapshot for public cache operation identities on the binary transport.</summary>
[Immutable]
public sealed class BinaryOperationGoldenSnapshotTests : ServerUnitTestBase
{
    /// <summary>Checks that operation names and wire IDs match the committed protocol snapshot.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public Task MatchesPublicBinaryOperations(CancellationToken cancellationToken)
    {
        var actual = new HashSet<string>(StringComparer.Ordinal);
        foreach (var operation in Enum.GetValues<BinaryOperation>())
            _ = actual.Add(string.Create(System.Globalization.CultureInfo.InvariantCulture, $"{Unsafe.BitCast<BinaryOperation, int>(operation)} {operation}"));

        var path = PathKit.Combine(AppContext.BaseDirectory, "ApiSnapshots", "SquirixBinaryOperations.golden.txt");
        return GoldenSnapshotAssert.MatchesAsync(actual, path, cancellationToken);
    }
}
