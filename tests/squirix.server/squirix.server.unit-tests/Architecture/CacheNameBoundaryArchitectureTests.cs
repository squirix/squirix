using ArchUnitNET.xUnitV3;
using Squirix.Server.Cluster;
using Squirix.Server.Runtime;
using Xunit;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Architecture rules for canonical cache name boundaries, validation ownership, and placement of routing versus local watch infrastructure.</summary>
public sealed class CacheNameBoundaryArchitectureTests
{
    /// <summary>Ensures key routing, runtime, and validation decorator types remain in their intended namespaces.</summary>
    [Fact]
    public void CacheBoundaryTypesShouldLiveInApprovedNamespaces()
    {
        var runtime = ServerArchitectureScope.Server.And().HaveName(nameof(CacheRuntime))
            .Should().ResideInNamespace(ServerArchitectureNamespaces.Runtime);
        runtime.Check(ServerArchitecture.Instance);

        var validation = ServerArchitectureScope.Server.And().HaveName("ValidationCacheDecorator`1")
            .Should().ResideInNamespace($"{ServerArchitectureNamespaces.Node}.App.Decorators");
        validation.Check(ServerArchitecture.Instance);

        var hasher = ServerArchitectureScope.Server.And().HaveName(nameof(Sha256Hasher))
            .Should().ResideInNamespace(typeof(Sha256Hasher).Namespace!);
        hasher.Check(ServerArchitecture.Instance);
    }
}
