using System.Threading;
using Xunit;

namespace Squirix.E2ETests.Infrastructure;

/// <summary>
/// Base class for SDK end-to-end tests.
/// </summary>
public abstract class E2ETestBase
{
    /// <summary>
    /// Gets the default cancellation token for the current test.
    /// </summary>
    protected static CancellationToken DefaultCancellationToken => TestContext.Current.CancellationToken;
}
