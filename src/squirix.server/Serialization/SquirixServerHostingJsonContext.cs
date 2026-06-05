using System.Text.Json;
using System.Text.Json.Serialization;

namespace Squirix.Server.Serialization;

/// <summary>
/// Source-generated JSON metadata for public server hosting configuration.
/// </summary>
[JsonSourceGenerationOptions(JsonSerializerDefaults.Web, PropertyNameCaseInsensitive = true, ReadCommentHandling = JsonCommentHandling.Skip, AllowTrailingCommas = true)]
[JsonSerializable(typeof(SquirixServerOptions))]
[JsonSerializable(typeof(SquirixServerPeerOptions))]
internal sealed partial class SquirixServerHostingJsonContext : JsonSerializerContext;
