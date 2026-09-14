using System.IO;
using Squirix.Server.Attributes;
using Xunit;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Guards shared recovery test infrastructure behavior.</summary>
[Immutable]
public sealed class RecoveryScenarioBuilderTests
{
    /// <summary>Verifies the shared recovery scenario owns and deletes its temporary directory.</summary>
    [Fact]
    public void DisposeDeletesTemporaryDirectory()
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-builder-guard");
        var dataDir = scenario.DataDir;

        Assert.True(Directory.Exists(dataDir));

        // ReSharper disable once DisposeOnUsingVariable
        scenario.Dispose();

        Assert.False(Directory.Exists(dataDir));
    }
}
