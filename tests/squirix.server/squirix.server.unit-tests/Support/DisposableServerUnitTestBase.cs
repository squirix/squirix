using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Xunit;

namespace Squirix.Server.UnitTests.Support;

/// <summary>
/// Base for server unit tests that own disposable resources for the test class lifetime.
/// Implements <see cref="IAsyncLifetime" /> and exposes <see cref="OnInitializeAsync" /> and
/// <see cref="DisposeManaged" /> so derived classes override lifecycle hooks instead of implementing disposal directly.
/// </summary>
[Immutable]
public abstract class DisposableServerUnitTestBase : ServerUnitTestBase, IAsyncLifetime
{
    /// <summary>Releases resources after the test class finishes.</summary>
    public ValueTask DisposeAsync()
    {
        DisposeManaged();
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    /// <summary>Creates resources before the test class runs.</summary>
    public ValueTask InitializeAsync() => OnInitializeAsync();

    /// <summary>Releases owned resources. Override to dispose fields created by the test class.</summary>
    protected virtual void DisposeManaged()
    {
    }

    /// <summary>Creates resources before the test class runs. Override to initialize state.</summary>
    protected virtual ValueTask OnInitializeAsync() => ValueTask.CompletedTask;
}
