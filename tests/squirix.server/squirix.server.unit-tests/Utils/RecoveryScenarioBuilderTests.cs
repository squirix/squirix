using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>
/// Guards shared recovery test infrastructure behavior.
/// </summary>
public sealed class RecoveryScenarioBuilderTests
{
    /// <summary>
    /// Verifies the shared recovery scenario owns and deletes its temporary directory.
    /// </summary>
    /// <returns>A <see cref="Task" /> that completes when assertions pass.</returns>
    [Fact]
    public async Task DisposeDeletesTemporaryDirectory()
    {
        var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-builder-guard");
        var dataDir = scenario.DataDir;

        Assert.True(Directory.Exists(dataDir));

        await scenario.DisposeAsync();

        Assert.False(Directory.Exists(dataDir));
    }
}
