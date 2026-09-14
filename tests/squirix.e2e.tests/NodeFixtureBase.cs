using System.Threading;
using Squirix.Attributes;
using Xunit;

namespace Squirix.E2ETests;

/// <summary>Base class for shared cluster xUnit class fixtures.</summary>
[Immutable]
public abstract class NodeFixtureBase
{
    /// <summary>Gets the default cancellation token.</summary>
    protected static CancellationToken DefaultCancellationToken => TestContext.Current.CancellationToken;
}
