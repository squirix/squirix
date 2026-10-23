using System.Threading.Tasks;
using Squirix.Server.Attributes;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Support;

/// <summary>
/// Base for server unit tests that own disposable resources for the test lifetime.
/// Exposes <see cref="OnInitializeAsync" /> and <see cref="DisposeManaged" /> so derived classes
/// override lifecycle hooks instead of implementing disposal directly.
/// </summary>
[Immutable]
public abstract class DisposableServerUnitTestBase : ServerUnitTestBase
{
    /// <summary>Releases resources after the test finishes.</summary>
    [After(HookType.Test)]
    public void DisposeAfterTest() => DisposeManaged();

    /// <summary>Creates resources before the test runs.</summary>
    [Before(HookType.Test)]
    public Task InitializeAsync() => OnInitializeAsync().AsTask();

    /// <summary>Releases owned resources. Override to dispose fields created by the test class.</summary>
    protected virtual void DisposeManaged()
    {
    }

    /// <summary>Creates resources before the test class runs. Override to initialize state.</summary>
    protected virtual ValueTask OnInitializeAsync() => ValueTask.CompletedTask;
}
