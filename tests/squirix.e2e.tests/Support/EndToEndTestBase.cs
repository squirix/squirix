using System.Threading;
using Xunit;

namespace Squirix.E2ETests.Support;

/// <summary>Base class for SDK end-to-end tests.</summary>
public abstract class EndToEndTestBase
{
    /// <summary>Gets the default cancellation token for the current test.</summary>
    protected static CancellationToken DefaultCancellationToken => TestContext.Current.CancellationToken;
}
