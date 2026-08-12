using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace Squirix.Server.Storage.Codecs;

/// <summary>Source-generated JSON metadata for node materialization.</summary>
[JsonSerializable(typeof(JsonNode))]
internal sealed partial class JsonTreeJsonContext : JsonSerializerContext;
