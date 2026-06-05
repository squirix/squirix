using System;
using System.Reflection;
using Squirix.Server.Node.App.Decorators;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Decorators;

/// <summary>
/// Ensures the tracing decorator does not publish a logical pipeline disposal surface.
/// </summary>
public sealed class TracingCacheDecoratorLifetimeTests
{
    /// <summary>
    /// Logical decorators must not declare <see cref="IAsyncDisposable.DisposeAsync" />.
    /// </summary>
    [Fact]
    public void TracingCacheDecoratorDoesNotDeclareDisposeAsync()
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
