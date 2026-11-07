using System.Text.Json.Serialization;

namespace Squirix.AspNetSharedCache;

/// <summary>Serializes the shared banner for the Squirix client.</summary>
[JsonSourceGenerationOptions(RespectNullableAnnotations = true, RespectRequiredConstructorParameters = true)]
[JsonSerializable(typeof(Banner))]
internal sealed partial class BannerJsonContext : JsonSerializerContext;
