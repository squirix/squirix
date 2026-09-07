using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace Squirix.Server.Storage.Codecs;

/// <summary>Source-generated JSON metadata for node materialization.</summary>
[JsonSourceGenerationOptions(RespectNullableAnnotations = false, RespectRequiredConstructorParameters = false)]
[JsonSerializable(typeof(JsonNode))]
internal sealed partial class JsonTreeJsonContext : JsonSerializerContext;
