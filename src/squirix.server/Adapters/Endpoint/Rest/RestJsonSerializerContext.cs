using System.Text.Json;
using System.Text.Json.Serialization;
using Squirix.Server.Adapters.Rest;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Adapters.Endpoint.Rest;

/// <summary>
/// Source-generated JSON metadata for public REST, admin, and health response DTOs.
/// </summary>
[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(RestDtos.AdminWhoamiResponse))]
[JsonSerializable(typeof(RestDtos.AdminOwnerResponse))]
[JsonSerializable(typeof(RestDtos.AdminRingResponse))]
[JsonSerializable(typeof(RestDtos.AdminRingNodeDistribution))]
[JsonSerializable(typeof(RestDtos.AdminOwnerLookupSample))]
[JsonSerializable(typeof(RestDtos.AdminAuditResponse))]
[JsonSerializable(typeof(RestDtos.AdminRebalanceHistoryResponse))]
[JsonSerializable(typeof(RestDtos.AdminRebalanceHistoryEvent))]
[JsonSerializable(typeof(RestDtos.AdminStorageDiagnosticsResponse))]
[JsonSerializable(typeof(AdminManifestSnapshot))]
[JsonSerializable(typeof(AdminManifestSnapshotRef))]
[JsonSerializable(typeof(RestDtos.AdminJournalWriterDiagnostics))]
[JsonSerializable(typeof(RestDtos.AdminJournalDiagnostics))]
[JsonSerializable(typeof(RestDtos.AdminJournalSegmentDiagnostic))]
[JsonSerializable(typeof(RestDtos.AdminMembersResponse))]
[JsonSerializable(typeof(RestDtos.AdminUnsupportedMutationResponse))]
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
