using System;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Core;

/// <summary>Covers the missing-metadata failures of the server serializer metadata chain.</summary>
public sealed class ServerSerializerMetadataTests
{
    /// <summary>A value of an unregistered type declared as object names both the runtime and declared type.</summary>
    /// <returns>A task that completes when the assertions finish.</returns>
    [Test]
    public async Task MissingRuntimeTypeNamesBothTypes()
    {
        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(
            new UnregisteredValue(1),
            static value => _ = ServerSerializerMetadata.SerializeToElement(value, typeof(object)));

        _ = await Assert.That(ex.Message).Contains($"No JSON metadata registered for {typeof(UnregisteredValue)} (runtime type of {typeof(object)}).");
    }

    /// <summary>A value whose unregistered declared type matches its runtime type names that type only.</summary>
    /// <returns>A task that completes when the assertions finish.</returns>
    [Test]
    public async Task MissingDeclaredTypeNamesThatType()
    {
        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(
            new UnregisteredValue(1),
            static value => _ = ServerSerializerMetadata.SerializeToElement(value, typeof(UnregisteredValue)));

        _ = await Assert.That(ex.Message).Contains($"No JSON metadata registered for {typeof(UnregisteredValue)}. Register");
    }

    private sealed record UnregisteredValue(int Id);
}
