using System.Text.Json;
using System.Text.Json.Serialization;
using Squirix.Server.Adapters.Rest;

namespace Squirix.Server.Adapters.Endpoint.Rest;

/// <summary>
/// Source-generated JSON metadata for public REST and health response DTOs.
/// </summary>
[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(RestDtos.RestIncrementResponse))]
[JsonSerializable(typeof(RestDtos.RestErrorResponse))]
[JsonSerializable(typeof(RestDtos.HealthReadyDetailsResponse))]
[JsonSerializable(typeof(RestDtos.HealthCompactionDetails))]
[JsonSerializable(typeof(RestDtos.HealthClientPoolDetails))]
[JsonSerializable(typeof(RestDtos.HealthCoordinationDetails))]
[JsonSerializable(typeof(RestDtos.HealthLeaseDetails))]
[JsonSerializable(typeof(RestDtos.HealthWatchDetails))]
[JsonSerializable(typeof(RestDtos.HealthMemoryPressureDetails))]
internal sealed partial class RestJsonSerializerContext : JsonSerializerContext;
