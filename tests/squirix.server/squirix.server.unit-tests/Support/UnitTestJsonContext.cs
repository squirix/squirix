using System.Runtime.CompilerServices;
using System.Text.Json.Serialization;
using Squirix.Server.Core;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Source-generated JSON metadata for unit-test payload DTOs (AOT-safe, no reflection).</summary>
/// <remarks>Registered with <see cref="ServerSerializerMetadata" /> at an assembly load so product
/// serializer paths resolve test DTOs through the metadata chain.</remarks>
[JsonSourceGenerationOptions(RespectNullableAnnotations = false, RespectRequiredConstructorParameters = false)]
[JsonSerializable(typeof(SamplePayload))]
[JsonSerializable(typeof(ValuePayload))]
[JsonSerializable(typeof(Core.DerivedValue))]
[JsonSerializable(typeof(Core.IdPayload))]
[JsonSerializable(typeof(Persistence.Journaling.Codec.DerivedValue), TypeInfoPropertyName = "JournalDerivedValue")]
[JsonSerializable(typeof(Memory.ObjectCacheDataPayload))]
[JsonSerializable(typeof(Memory.AdmissionDataPayload), TypeInfoPropertyName = "AdmissionDataPayload")]
internal sealed partial class UnitTestJsonContext : JsonSerializerContext
{
    /// <summary>Registers test metadata with the product serializer chain.</summary>
    [ModuleInitializer]
    internal static void RegisterUnitTestMetadata()
    {
        ServerSerializerMetadata.RegisterContext(SquirixServerHostingJsonContext.Default);
        ServerSerializerMetadata.RegisterContext(Default);
    }
}
