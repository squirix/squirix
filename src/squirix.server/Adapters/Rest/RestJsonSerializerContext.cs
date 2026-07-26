using System.Text.Json;
using System.Text.Json.Serialization;

namespace Squirix.Server.Adapters.Rest;

/// <summary>Source-generated JSON metadata for health/metrics HTTP response DTOs and shared error payloads.</summary>
[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(IncrementResponse))]
[JsonSerializable(typeof(ErrorResponse))]
[JsonSerializable(typeof(HealthReadyDetailsResponse))]
[JsonSerializable(typeof(HealthCompactionDetails))]
[JsonSerializable(typeof(HealthClientPoolDetails))]
[JsonSerializable(typeof(HealthCoordinationDetails))]
[JsonSerializable(typeof(HealthLeaseDetails))]
[JsonSerializable(typeof(HealthWatchDetails))]
[JsonSerializable(typeof(HealthMemoryPressureDetails))]
[JsonSerializable(typeof(HealthJournalDiskDetails))]
[JsonSerializable(typeof(HealthRetentionCleanupDetails))]
internal sealed partial class RestJsonSerializerContext : JsonSerializerContext;
