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
    /// <summary>An asynchronous callback that faults hands its exception back instead of letting it escape.</summary>
    [Test]
    public async Task RunAsyncReturnsFault()
    {
        var thrown = new InvalidOperationException("Callback failed.");

        var failure = await Isolated.RunAsync(thrown, static exception => ValueTask.FromException(exception));

        _ = await Assert.That(failure).IsSameReferenceAs(thrown);
    }

    /// <summary>An asynchronous callback that throws before returning its task hands that exception back as well.</summary>
    [Test]
    public async Task RunAsyncReturnsSynchronousThrow()
    {
        var thrown = new InvalidOperationException("Callback failed.");

        var failure = await Isolated.RunAsync(thrown, static exception => throw exception);

        _ = await Assert.That(failure).IsSameReferenceAs(thrown);
    }

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

        var failure = Isolated.Run(thrown, Throw);

        _ = await Assert.That(failure).IsSameReferenceAs(thrown);
    }

    private static void Throw(InvalidOperationException exception) => throw exception;
}
