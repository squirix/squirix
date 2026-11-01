using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Squirix.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Exceptions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Verifies that synchronous assertions reject the root exception type before invoking the operation.</summary>
public sealed class ExceptionAssertTests
{
    /// <summary>The public factory rejects System.Exception as an expected type.</summary>
    [Test]
    public async Task RootFactoryIsRejected()
    {
        var failure = ExceptionAssert.For<AssertionException>().Throws(
            0,
            static state =>
            {
                _ = state;
                _ = ExceptionAssert.For<Exception>();
            });
        _ = await Assert.That(failure.Message).IsEqualTo("System.Exception cannot be used as the expected type. Specify a more specific exception type.");
    }

    /// <summary>Default construction cannot bypass validation, and an invalid assertion never invokes its operation.</summary>
    /// <param name="kind">The synchronous assertion overload.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    public async Task DefaultStructRejectsRoot(int kind)
    {
        var calls = new StrongBox<int>();
        var failure = ExceptionAssert.For<AssertionException>().Throws((Kind: kind, Calls: calls), static state => InvokeRootAssertion(state.Kind, state.Calls));
        _ = await Assert.That(failure.Message).IsEqualTo("System.Exception cannot be used as the expected type. Specify a more specific exception type.");
        _ = await Assert.That(calls.Value).IsEqualTo(0);
    }

    private static void InvokeRootAssertion(int kind, StrongBox<int> calls)
    {
        if (kind == 0)
        {
            _ = default(ExceptionExpectation<Exception>).Throws(calls, static state => state.Value++);
            return;
        }

        _ = default(ExceptionExpectation<Exception>).ThrowsAny(calls, static state => state.Value++);
    }
}
