using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Checks shared protobuf sources and assembly-local wire contracts.</summary>
[Immutable]
public sealed class ServerWireArchitectureTests : ServerUnitTestBase
{
    /// <summary>Ensures both packages compile the shared cache wire encoding from the same source.</summary>
    [Test]
    public async Task ProjectsShareCacheWireEncoding()
    {
        const string include = @"..\shared\Squirix\Transport\Generated\SquirixCache.cs";
        var serverIncludes = ServerArchitectureFixtures.GetServerProjectIndex().GetIncludes("Compile");
        var clientProject = await ServerArchitectureFixtures.LoadProjectAsync("src/squirix/Squirix.csproj");
        var clientIncludes = ServerArchitectureFixtures.ParseMsbuildProject(clientProject).GetIncludes("Compile");

        _ = await Assert.That(serverIncludes).IsNotNull();
        _ = await Assert.That(clientIncludes).IsNotNull();
        _ = await Assert.That(serverIncludes).Contains(include, StringComparer.Ordinal);
        _ = await Assert.That(clientIncludes).Contains(include, StringComparer.Ordinal);
    }

    /// <summary>Ensures wire messages remain internal to the package.</summary>
    [Test]
    public async Task WireMessagesAreInternal() => _ = await Assert.That(typeof(CacheEntryWire).IsPublic).IsFalse();
}
