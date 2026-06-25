using System;
using Squirix.Server.Node.App.Decorators;
using Xunit;

namespace Squirix.Server.UnitTests.Core;

/// <summary>Ensures the tracing decorator does not publish a logical pipeline disposal surface.</summary>
public sealed class TracingCacheDecoratorLifetimeTests
{
    /// <summary>
    /// Logical decorators must not declare <see cref="IAsyncDisposable.DisposeAsync" />.
    /// </summary>
    [Fact]
    public void TracingCacheDecoratorDoesNotDeclareDispose()
    {
        var m = typeof(TracingCacheDecorator<>).GetMethod(
            nameof(IAsyncDisposable.DisposeAsync),
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly,
            null,
            Type.EmptyTypes,
            null);

        Assert.Null(m);
    }
}
