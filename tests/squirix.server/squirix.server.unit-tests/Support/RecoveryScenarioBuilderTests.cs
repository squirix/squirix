using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Guards shared recovery test infrastructure behavior.</summary>
[Immutable]
public sealed class RecoveryScenarioBuilderTests
{
    /// <summary>Verifies the shared recovery scenario owns and deletes its temporary directory.</summary>
    [Test]
    public async Task DisposeDeletesTemporaryDirectory()
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-builder-guard");
        var dataDir = scenario.DataDir;

        _ = await Assert.That(Directory.Exists(dataDir)).IsTrue();

        // ReSharper disable once DisposeOnUsingVariable
        scenario.Dispose();

        _ = await Assert.That(Directory.Exists(dataDir)).IsFalse();
    }
}
