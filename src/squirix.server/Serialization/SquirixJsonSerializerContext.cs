using System.Text.Json;
using System.Text.Json.Serialization;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Json;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Serialization;

/// <summary>
/// Source-generated JSON metadata for squirix persistence DTOs.
/// </summary>
[JsonSourceGenerationOptions(
    JsonSerializerDefaults.Web,
    PropertyNameCaseInsensitive = true,
    ReadCommentHandling = JsonCommentHandling.Skip,
    AllowTrailingCommas = true,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
[JsonSerializable(typeof(Manifest))]
[JsonSerializable(typeof(Manifest.SnapshotRef))]
[JsonSerializable(typeof(SnapshotFrame))]
[JsonSerializable(typeof(PersistedIdempotencyRecord))]
[JsonSerializable(typeof(PersistedIdempotencyOutcome))]
[JsonSerializable(typeof(RecordEnvelope))]
[JsonSerializable(typeof(PutOp))]
[JsonSerializable(typeof(RemoveOp))]
[JsonSerializable(typeof(RemoveExpirationOp))]
[JsonSerializable(typeof(TouchExpirationOp))]
[JsonSerializable(typeof(ItemPair))]
internal sealed partial class SquirixJsonSerializerContext : JsonSerializerContext;
