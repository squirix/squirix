using System;
using System.Threading.Tasks;
using Squirix.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Covers the missing-metadata failures of the client serializer metadata chain.</summary>
public sealed class ClientSerializerMetadataTests
{
    /// <summary>A value of an unregistered type declared as object names both the runtime and declared type.</summary>
    /// <returns>A task that completes when the assertions finish.</returns>
    [Test]
    public async Task MissingRuntimeTypeNamesBothTypes()
    {
        var ex = ExceptionAssert.For<InvalidOperationException>().Throws(
            new UnregisteredValue(1),
            static value => _ = ClientSerializerMetadata.SerializeToUtf8Bytes(value, typeof(object)));

        _ = await Assert.That(ex.Message).Contains($"No JSON metadata registered for {typeof(UnregisteredValue)} (runtime type of {typeof(object)}).");
    }

    /// <summary>A value whose unregistered declared type matches its runtime type names that type only.</summary>
    /// <returns>A task that completes when the assertions finish.</returns>
    [Test]
    public async Task MissingDeclaredTypeNamesThatType()
    {
        var ex = ExceptionAssert.For<InvalidOperationException>().Throws(
            new UnregisteredValue(1),
            static value => _ = ClientSerializerMetadata.SerializeToUtf8Bytes(value, typeof(UnregisteredValue)));

        _ = await Assert.That(ex.Message).Contains($"No JSON metadata registered for {typeof(UnregisteredValue)}. Register");
    }

    private sealed record UnregisteredValue(int Id);
}
