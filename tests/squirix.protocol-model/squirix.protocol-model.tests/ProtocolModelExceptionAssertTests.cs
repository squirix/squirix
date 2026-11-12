using System;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Exceptions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

/// <summary>Verifies the root-type restriction in the protocol-model assertion helper.</summary>
public sealed class ProtocolModelExceptionAssertTests
{
    /// <summary>The factory rejects the root type with the explicit policy failure.</summary>
    [Test]
    public async Task RootFactoryIsRejected()
    {
        var failure = ProtocolModelExceptionAssert.For<AssertionException>().Throws(static () => _ = ProtocolModelExceptionAssert.For<Exception>());
        _ = await Assert.That(failure.Message).IsEqualTo("System.Exception cannot be used as the expected type. Specify a more specific exception type.");
    }

    /// <summary>A default expectation cannot bypass validation.</summary>
    [Test]
    public async Task DefaultStructRejectsRoot()
    {
        var failure = ProtocolModelExceptionAssert.For<AssertionException>().Throws(static () =>
            _ = default(ProtocolModelExceptionExpectation<Exception>).Throws(static () => throw new InvalidOperationException("operation must not run")));
        _ = await Assert.That(failure.Message).IsEqualTo("System.Exception cannot be used as the expected type. Specify a more specific exception type.");
    }
}
