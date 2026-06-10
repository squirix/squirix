using System;
using System.Text.Json.Serialization;

namespace Squirix.Server.Adapters.Rest;

internal static class RestDtos
{
    internal sealed class HealthClientPoolDetails
    {
        public HealthClientPoolDetails(bool configured, int peers)
        {
            Configured = configured;
            Peers = peers;
        }

        public bool Configured { get; }

        public int Peers { get; }
    }

    internal sealed class HealthCompactionDetails
    {
        public HealthCompactionDetails(string state, DateTime? lastRunUtc, bool inFlight)
        {
            State = state;
            LastRunUtc = lastRunUtc;
            InFlight = inFlight;
        }

        public bool InFlight { get; }

        public DateTime? LastRunUtc { get; }

        public string State { get; }
    }

    internal sealed class HealthCoordinationDetails
    {
        public HealthCoordinationDetails(HealthLeaseDetails leases, HealthWatchDetails watches)
        {
            Leases = leases;
            Watches = watches;
        }

        public HealthLeaseDetails Leases { get; }

        public HealthWatchDetails Watches { get; }
    }

    internal sealed class HealthLeaseDetails
    {
        public HealthLeaseDetails(bool configured, int active, int expired, int renewals)
        {
            Configured = configured;
            Active = active;
            Expired = expired;
            Renewals = renewals;
        }

        public int Active { get; }

        public bool Configured { get; }

        public int Expired { get; }

        public int Renewals { get; }
    }

    internal sealed class HealthMemoryPressureDetails
    {
        public HealthMemoryPressureDetails(
            string state,
            long? maxEstimatedCacheBytes,
            long estimatedCacheBytes,
            long entryCount,
            long rejectedWriteCount,
            bool writeRejectionActive)
        {
            State = state;
            MaxEstimatedCacheBytes = maxEstimatedCacheBytes;
            EstimatedCacheBytes = estimatedCacheBytes;
            EntryCount = entryCount;
            RejectedWriteCount = rejectedWriteCount;
            WriteRejectionActive = writeRejectionActive;
        }

        public long EntryCount { get; }

        public long EstimatedCacheBytes { get; }

        public long? MaxEstimatedCacheBytes { get; }

        public long RejectedWriteCount { get; }

        public string State { get; }

        public bool WriteRejectionActive { get; }
    }

    internal sealed class HealthReadyDetailsResponse
    {
        public HealthReadyDetailsResponse(
            long journalBacklogOps,
            double? snapshotAgeSeconds,
            bool snapshotInFlight,
            HealthCompactionDetails compaction,
            HealthClientPoolDetails clientPool,
            HealthCoordinationDetails coordination,
            HealthMemoryPressureDetails memoryPressure)
        {
            JournalBacklogOps = journalBacklogOps;
            SnapshotAgeSeconds = snapshotAgeSeconds;
            SnapshotInFlight = snapshotInFlight;
            Compaction = compaction;
            ClientPool = clientPool;
            Coordination = coordination;
            MemoryPressure = memoryPressure;
        }

        public HealthClientPoolDetails ClientPool { get; }

        public HealthCompactionDetails Compaction { get; }

        public HealthCoordinationDetails Coordination { get; }

        [JsonPropertyName("journalBacklogOps")]
        public long JournalBacklogOps { get; }

        public HealthMemoryPressureDetails MemoryPressure { get; }

        public double? SnapshotAgeSeconds { get; }

        public bool SnapshotInFlight { get; }
    }

    internal sealed class HealthWatchDetails
    {
        public HealthWatchDetails(bool configured, int active, int droppedEvents, int bufferedEvents)
        {
            Configured = configured;
            Active = active;
            DroppedEvents = droppedEvents;
            BufferedEvents = bufferedEvents;
        }

        public int Active { get; }

        public int BufferedEvents { get; }

        public bool Configured { get; }

        public int DroppedEvents { get; }
    }

    internal sealed class RestErrorResponse
    {
        public RestErrorResponse(string error, string code, string? detail)
        {
            Error = error;
            Code = code;
            Detail = detail;
        }

        public string Code { get; }

        public string? Detail { get; }

        public string Error { get; }
    }

    internal sealed class RestIncrementResponse
    {
        public RestIncrementResponse(long value)
        {
            Value = value;
        }

        public long Value { get; }
    }
}
