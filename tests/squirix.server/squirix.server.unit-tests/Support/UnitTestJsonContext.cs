using System.Runtime.CompilerServices;
using System.Text.Json.Serialization;
using Squirix.Server.Core;
using Squirix.Server.UnitTests.Core;
using Squirix.Server.UnitTests.Memory;
using Squirix.Server.UnitTests.Persistence.Journaling.Codec;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Source-generated JSON metadata for unit-test payload DTOs (AOT-safe, no reflection).</summary>
/// <remarks>Registered with <see cref="SerializerMetadata" /> at assembly load so product
/// serializer paths resolve test DTOs through the metadata chain.</remarks>
[JsonSourceGenerationOptions(RespectNullableAnnotations = false, RespectRequiredConstructorParameters = false)]
[JsonSerializable(typeof(CacheValueGrpcMappingTests.SamplePayload))]
[JsonSerializable(typeof(CacheValueGrpcMappingTests.ValuePayload))]
[JsonSerializable(typeof(NodeCacheEntryTests.DerivedValue))]
[JsonSerializable(typeof(NodeCacheEntryTests.IdPayload))]
[JsonSerializable(typeof(JournalEntryPayloadTests.DerivedValue), TypeInfoPropertyName = "JournalDerivedValue")]
[JsonSerializable(typeof(ObjectCacheEntrySizeEstimatorTests.DataPayload))]
[JsonSerializable(typeof(AdmissionObjectEntryTests.DataPayload), TypeInfoPropertyName = "AdmissionDataPayload")]
internal sealed partial class UnitTestJsonContext : JsonSerializerContext
{
    /// <summary>Registers test metadata with the product serializer chain.</summary>
    [ModuleInitializer]
    internal static void RegisterUnitTestMetadata() => SerializerMetadata.RegisterContext(Default);
}
