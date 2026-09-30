using System;
using System.Threading.Tasks;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Threading;

/// <summary>Failure hand-back of <see cref="Isolated" />.</summary>
public sealed class IsolatedTests : ServerUnitTestBase
{
    /// <summary>A callback that completes yields no failure.</summary>
    [Test]
    public async Task RunReturnsNullOnSuccess()
    {
        var failure = Isolated.Run(1, static _ => { });

        _ = await Assert.That(failure).IsNull();
    }

    /// <summary>A callback that throws hands its exception back instead of letting it escape.</summary>
    [Test]
    public async Task RunReturnsThrownException()
    {
        var thrown = new InvalidOperationException("Callback failed.");

        var failure = Isolated.Run(thrown, static exception => throw exception);

        _ = await Assert.That(failure).IsSameReferenceAs(thrown);
    }
}
