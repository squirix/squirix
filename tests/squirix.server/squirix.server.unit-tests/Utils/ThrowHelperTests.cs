using System;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using Xunit;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>Contract tests for <see cref="ThrowHelper" /> guards.</summary>
[Immutable]
public sealed class ThrowHelperTests : ServerUnitTestBase
{
    /// <summary>Required returns the value when it is not null.</summary>
    [Fact]
    public void RequiredReturnsValue() => Assert.Equal("v", ThrowHelper.Required<string>("v", "boom"));

    /// <summary>Required throws with the message when the value is null.</summary>
    [Fact]
    public void RequiredThrowsOnNull()
    {
        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws("boom", static message => ThrowHelper.Required<string>(null, message));

        Assert.Equal("boom", ex.Message);
    }

    /// <summary>RequiredValue returns the value when it has a value.</summary>
    [Fact]
    public void RequiredValueReturnsValue() => Assert.Equal(7, ThrowHelper.RequiredValue<int>(7, "boom"));

    /// <summary>RequiredValue throws with the message when there is no value.</summary>
    [Fact]
    public void RequiredValueThrowsOnNull()
    {
        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws("boom", static message => ThrowHelper.RequiredValue<int>(null, message));

        Assert.Equal("boom", ex.Message);
    }

    /// <summary>Throw raises the given exception for expression-embedded use.</summary>
    [Fact]
    public void ThrowRaisesException()
    {
        var expected = new InvalidOperationException("boom");

        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(expected, static e => ThrowHelper.Throw<string>(e));

        Assert.Same(expected, ex);
    }
}
