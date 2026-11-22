using System.Text.Json.Serialization;

namespace Squirix.Server.Benchmarks;

/// <summary>Source-generated metadata for the Base64 comparison path.</summary>
[JsonSourceGenerationOptions(RespectNullableAnnotations = true, RespectRequiredConstructorParameters = true)]
[JsonSerializable(typeof(byte[]))]
internal sealed partial class BinaryBenchmarkJson : JsonSerializerContext;
