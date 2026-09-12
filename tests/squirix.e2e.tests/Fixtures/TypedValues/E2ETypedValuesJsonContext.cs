using System.Runtime.CompilerServices;
using System.Text.Json.Serialization;
using Squirix.Server.TestKit.Hosting;

namespace Squirix.E2ETests.Fixtures.TypedValues;

/// <summary>Source-generated JSON metadata for e2e typed-value fixtures (AOT-safe, no reflection).</summary>
/// <remarks>Registered with the client and server serializer chains at assembly load,
/// modeling how consuming applications register their own payload contexts.</remarks>
[JsonSourceGenerationOptions(RespectNullableAnnotations = false, RespectRequiredConstructorParameters = false)]
[JsonSerializable(typeof(TypedMutableCart))]
[JsonSerializable(typeof(TypedCartItem))]
[JsonSerializable(typeof(TypedCustomerProfile))]
[JsonSerializable(typeof(TypedCustomerAddress))]
[JsonSerializable(typeof(TypedCustomerStatus))]
internal sealed partial class E2ETypedValuesJsonContext : JsonSerializerContext
{
    /// <summary>Registers fixture metadata with the product serializer chains.</summary>
    [ModuleInitializer]
    internal static void RegisterE2EMetadata()
    {
        Squirix.SerializerMetadata.RegisterContext(Default);
        PayloadContextRegistration.RegisterServerPayloadContext(Default);
    }
}
