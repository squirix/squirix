using System.IO;
using System.Threading.Tasks;
using Squirix.Attributes;
using Xunit;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Guards shared recovery test infrastructure behavior.</summary>
[Immutable]
public sealed class RecoveryScenarioBuilderTests
{
    /// <summary>Verifies the shared recovery scenario owns and deletes its temporary directory.</summary>
    [Fact]
    public async Task DisposeDeletesTemporaryDirectoryAsync()
    {
        var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-builder-guard");
        var dataDir = scenario.DataDir;

        Assert.True(Directory.Exists(dataDir));

        await scenario.DisposeAsync();

        Assert.False(Directory.Exists(dataDir));
    }
}
