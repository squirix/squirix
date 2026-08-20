using System.Threading;
using Xunit;

namespace Squirix.ProtocolModel.Tests;

/// <summary>Provides a common base for protocol-model tests, exposing a default <see cref="CancellationToken" />.</summary>
public abstract class ProtocolModelTestBase
{
    /// <summary>Gets a default <see cref="CancellationToken" /> with a 30s timeout.</summary>
    protected static CancellationToken DefaultCancellationToken => TestContext.Current.CancellationToken;
}
