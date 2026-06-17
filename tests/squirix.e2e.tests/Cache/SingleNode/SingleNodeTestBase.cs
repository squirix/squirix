using System;
using System.Diagnostics.CodeAnalysis;
using Squirix.E2ETests.Support;
using Squirix.E2ETests.Support.Cluster.Fixtures;
using Xunit;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>
/// Shared fixtures for single-node v0.1 public <see cref="ICache{T}" /> integration tests.
/// </summary>
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "Unit test base class must be public")]
public abstract class SingleNodeTestBase : EndToEndTestBase, IClassFixture<SingleNodeFixture>
{
    internal static readonly TimeSpan Delay60 = TimeSpan.FromMilliseconds(60);
    internal static readonly TimeSpan Delay90 = TimeSpan.FromMilliseconds(90);

    private readonly SingleNodeFixture _fixture;

    /// <summary>
    /// Initializes a new instance of the <see cref="SingleNodeTestBase" /> class.
    /// </summary>
    /// <param name="fixture">Shared single-node cluster fixture.</param>
    protected SingleNodeTestBase(SingleNodeFixture fixture) => _fixture = fixture ?? throw new ArgumentNullException(nameof(fixture));

    /// <summary>Gets the shared SDK client connected to the class cluster.</summary>
    protected ISquirixClient Client => _fixture.Client;
}
