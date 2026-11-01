using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Unit tests for the literal-friendly <see cref="SequenceAssert" /> overload.</summary>
[Immutable]
public sealed class SequenceAssertTests : ServerUnitTestBase
{
    /// <summary>A differing element fails the assertion.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SpanOverloadFailsOnElementMismatch(CancellationToken cancellationToken)
    {
        var actual = new[] { 1, 2, 4 };
        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(SequenceAssert.EqualAsync([1, 2, 3], actual), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
    }

    /// <summary>A different length fails the assertion.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SpanOverloadFailsOnLengthMismatch(CancellationToken cancellationToken)
    {
        var actual = new[] { 1, 2 };
        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(SequenceAssert.EqualAsync([1, 2, 3], actual), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
    }

    /// <summary>The comparer overload honors the supplied comparer for both matches and mismatches.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SpanOverloadUsesSuppliedComparer(CancellationToken cancellationToken)
    {
        var actual = new[] { "A", "b" };
        await SequenceAssert.EqualAsync<string>(["a", "B"], actual, StringComparer.OrdinalIgnoreCase);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(SequenceAssert.EqualAsync<string>(["a", "B"], actual, StringComparer.Ordinal), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
    }

    /// <summary>Equal literal and actual items pass.</summary>
    [Test]
    public Task SpanOverloadPassesOnEqualItems()
    {
        var actual = new[] { 1, 2, 3 };
        return SequenceAssert.EqualAsync([1, 2, 3], actual);
    }
}
