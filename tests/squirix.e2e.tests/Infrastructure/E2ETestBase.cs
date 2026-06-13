using System.Diagnostics.CodeAnalysis;
using System.Threading;
using Xunit;

namespace Squirix.E2ETests.Infrastructure;

/// <summary>
/// Base class for SDK end-to-end tests.
/// </summary>
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "Unit test base class must be public")]
public abstract class E2ETestBase
{
    /// <summary>
    /// Gets the default cancellation token for the current test.
    /// </summary>
    protected static CancellationToken DefaultCancellationToken => TestContext.Current.CancellationToken;
}
