using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Exceptions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Verifies that synchronous assertions reject the root exception type before invoking the operation.</summary>
public sealed class NodeExceptionAssertTests
{
    /// <summary>The public factory rejects System.Exception as an expected type.</summary>
    [Test]
    public async Task RootFactoryIsRejected()
    {
        var failure = NodeExceptionAssert.For<AssertionException>().Throws(
            0,
            static state =>
            {
                _ = state;
                _ = NodeExceptionAssert.For<Exception>();
            });
        _ = await Assert.That(failure.Message).IsEqualTo("System.Exception cannot be used as the expected type. Specify a more specific exception type.");
    }

    /// <summary>Default construction cannot bypass validation, and an invalid assertion never invokes its operation.</summary>
    /// <param name="kind">The synchronous assertion overload.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    [Arguments(4)]
    public async Task DefaultStructRejectsRoot(int kind)
    {
        var calls = new StrongBox<int>();
        var failure = NodeExceptionAssert.For<AssertionException>().Throws((Kind: kind, Calls: calls), static state => InvokeRootAssertion(state.Kind, state.Calls));
        _ = await Assert.That(failure.Message).IsEqualTo("System.Exception cannot be used as the expected type. Specify a more specific exception type.");
        _ = await Assert.That(calls.Value).IsEqualTo(0);
    }

    private static void InvokeRootAssertion(int kind, StrongBox<int> calls)
    {
        if (kind == 0)
        {
            _ = default(NodeExceptionExpectation<Exception>).Throws(static () => throw new InvalidOperationException("operation must not run"));
            return;
        }

        if (kind == 1)
        {
            _ = default(NodeExceptionExpectation<Exception>).Throws(calls, static state => state.Value++);
            return;
        }

        if (kind == 2)
        {
            _ = default(NodeExceptionExpectation<Exception>).Throws(calls, 1, static (state, amount) => state.Value += amount);
            return;
        }

        if (kind == 3)
        {
            _ = default(NodeExceptionExpectation<Exception>).Throws(calls, 1, 2, static (state, left, right) => state.Value += left + right);
            return;
        }

        _ = default(NodeExceptionExpectation<Exception>).ThrowsAny(calls, static state => state.Value++);
    }
}
