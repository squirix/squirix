using System.Text.Json.Serialization;

namespace Squirix.E2ETests.Fixtures;

/// <summary>Source-generated JSON metadata of the failover timing evidence file.</summary>
[JsonSourceGenerationOptions(
    WriteIndented = true,
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    RespectNullableAnnotations = false,
    RespectRequiredConstructorParameters = false)]
[JsonSerializable(typeof(FailoverTimingEvidence))]
internal sealed partial class FailoverEvidenceJsonContext : JsonSerializerContext;
