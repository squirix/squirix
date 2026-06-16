using System.Diagnostics.CodeAnalysis;
using System.Threading;
using Xunit;

namespace Squirix.E2ETests.Support.Cluster.Fixtures;

/// <summary>Base class for shared cluster xUnit class fixtures.</summary>
[SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Instantiated by xUnit via IClassFixture<T>.")]
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "Test fixture surface must be public for xUnit class fixtures.")]
public abstract class NodeFixtureBase
{
    /// <summary>Gets the default cancellation token.</summary>
    protected static CancellationToken DefaultCancellationToken => TestContext.Current.CancellationToken;
}
