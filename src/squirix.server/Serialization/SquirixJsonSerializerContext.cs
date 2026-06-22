using System.Text.Json;
using System.Text.Json.Serialization;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Snapshot.Json;

namespace Squirix.Server.Serialization;

/// <summary>Source-generated JSON metadata for squirix persistence DTOs.</summary>
[JsonSourceGenerationOptions(
    JsonSerializerDefaults.Web,
    PropertyNameCaseInsensitive = true,
    ReadCommentHandling = JsonCommentHandling.Skip,
    AllowTrailingCommas = true,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
[JsonSerializable(typeof(SnapshotFrame))]
[JsonSerializable(typeof(PersistedIdempotencyRecord))]
[JsonSerializable(typeof(PersistedIdempotencyOutcome))]
internal sealed partial class SquirixJsonSerializerContext : JsonSerializerContext;
