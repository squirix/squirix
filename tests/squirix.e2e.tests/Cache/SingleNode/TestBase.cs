using System;
using Squirix.Attributes;
using Squirix.Client;
using Xunit;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>
/// Shared fixtures for single-node v0.1 public <see cref="ICache{T}" /> integration tests.
/// </summary>
[Immutable]
public abstract class TestBase : EndToEndTestBase, IClassFixture<SingleNodeFixture>
{
    private readonly SingleNodeFixture _fixture;

    /// <summary>
    /// Initializes a new instance of the <see cref="TestBase" /> class.
    /// </summary>
    /// <param name="fixture">Shared single-node cluster fixture.</param>
    protected TestBase(SingleNodeFixture fixture)
    {
        _fixture = fixture ?? throw new ArgumentNullException(nameof(fixture));
    }

    /// <summary>Gets the shared SDK client connected to the class cluster.</summary>
    protected ISquirixClient Client => _fixture.Client;
}
