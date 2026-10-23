using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Contract tests for <see cref="ThrowHelper" /> guards.</summary>
[Immutable]
public sealed class ThrowHelperTests : ServerUnitTestBase
{
    /// <summary>Required returns the value when it is not null.</summary>
    [Test]
    public async Task RequiredReturnsValue() => _ = await Assert.That(ThrowHelper.Required<string>("v", "boom")).IsEqualTo("v");

    /// <summary>Required throws with the message when the value is null.</summary>
    [Test]
    public async Task RequiredThrowsOnNull()
    {
        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws("boom", static message => ThrowHelper.Required<string>(null, message));

        _ = await Assert.That(ex.Message).IsEqualTo("boom");
    }

    /// <summary>Throw raises the given exception for expression-embedded use.</summary>
    [Test]
    public async Task ThrowRaisesException()
    {
        var expected = new InvalidOperationException("boom");

        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(expected, static e => ThrowHelper.Throw<string>(e));

        _ = await Assert.That(ex).IsSameReferenceAs(expected);
    }
}
