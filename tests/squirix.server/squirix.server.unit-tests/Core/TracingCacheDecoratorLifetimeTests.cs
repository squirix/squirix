using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.App.Decorators;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Core;

/// <summary>Ensures the tracing decorator does not publish a logical pipeline disposal surface.</summary>
[Immutable]
public sealed class TracingCacheDecoratorLifetimeTests
{
    /// <summary>Logical decorators must not declare <see cref="IAsyncDisposable.DisposeAsync" />.</summary>
    [Test]
    public async Task TracingDecoratorDeclaresNoDispose() => _ = await Assert.That(typeof(IAsyncDisposable).IsAssignableFrom(typeof(TracingCacheDecorator<int>))).IsFalse();
}
