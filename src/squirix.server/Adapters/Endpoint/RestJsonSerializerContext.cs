using System.Text.Json;
using System.Text.Json.Serialization;
using Squirix.Server.Adapters.Rest;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Source-generated JSON metadata for public REST and health response DTOs.</summary>
[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(Dtos.RestIncrementResponse))]
[JsonSerializable(typeof(Dtos.RestErrorResponse))]
[JsonSerializable(typeof(Dtos.HealthReadyDetailsResponse))]
[JsonSerializable(typeof(Dtos.HealthCompactionDetails))]
[JsonSerializable(typeof(Dtos.HealthClientPoolDetails))]
[JsonSerializable(typeof(Dtos.HealthCoordinationDetails))]
[JsonSerializable(typeof(Dtos.HealthLeaseDetails))]
[JsonSerializable(typeof(Dtos.HealthWatchDetails))]
[JsonSerializable(typeof(Dtos.HealthMemoryPressureDetails))]
[JsonSerializable(typeof(Dtos.HealthRetentionCleanupDetails))]
internal sealed partial class RestJsonSerializerContext : JsonSerializerContext;
