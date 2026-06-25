using System.Diagnostics.CodeAnalysis;
using System.Threading;
using Xunit;

namespace Squirix.E2ETests.Support.Cluster.Fixtures;

/// <summary>Base class for shared cluster xUnit class fixtures.</summary>
[SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Instantiated by xUnit via IClassFixture<T>.")]
public abstract class NodeFixtureBase
{
    /// <summary>Gets the default cancellation token.</summary>
    protected static CancellationToken DefaultCancellationToken => TestContext.Current.CancellationToken;
}
