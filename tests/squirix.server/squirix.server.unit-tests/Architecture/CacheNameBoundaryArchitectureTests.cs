using NetArchTest.Rules;
using Squirix.Server.Cluster;
using Squirix.Server.Runtime;
using Xunit;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>
/// Architecture rules for canonical cache name boundaries, validation ownership, and placement of routing versus local watch infrastructure.
/// </summary>
public sealed class CacheNameBoundaryArchitectureTests
{
    /// <summary>
    /// Ensures key routing, runtime, watch hub, and validation decorator types remain in their intended namespaces (NetArchTest-backed placement checks).
    /// </summary>
    [Fact]
    public void ArchitectureRulesUseNetArchTestWherePossible()
    {
        var asm = SquirixArchitecture.ServerAssembly;

        var watchHub = Types.InAssembly(asm).That().HaveName("WatchHub`1").Should().ResideInNamespace(ServerArchitectureNamespaces.LocalCache).GetResult();
        ArchitectureAssertions.AssertArchitecture(watchHub);

        var runtime = Types.InAssembly(asm).That().HaveName(nameof(CacheRuntime)).Should().ResideInNamespace(ServerArchitectureNamespaces.Runtime).GetResult();
        ArchitectureAssertions.AssertArchitecture(runtime);

        var validation = Types.InAssembly(asm).That().HaveName("ValidationCacheDecorator`1").Should().ResideInNamespace($"{ServerArchitectureNamespaces.Node}.App.Decorators")
                              .GetResult();
        ArchitectureAssertions.AssertArchitecture(validation);

        var hasher = Types.InAssembly(asm).That().HaveName(nameof(Sha256Hasher)).Should().ResideInNamespace(typeof(Sha256Hasher).Namespace).GetResult();
        ArchitectureAssertions.AssertArchitecture(hasher);
    }
}
