using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;
using Squirix.Server.Adapters.Endpoint.Rest;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Adapters.Rest;

internal static class RestDtos
{
    internal sealed class AdminAuditResponse
    {
        public AdminAuditResponse(AdminAuditEvent[] events)
        {
            Events = events;
        }

        public AdminAuditEvent[] Events { get; }
    }

    internal sealed class AdminMembersResponse
    {
        public AdminMembersResponse(IReadOnlyCollection<string> members, int vnodes)
        {
            Members = members;
            Vnodes = vnodes;
        }

        public IReadOnlyCollection<string> Members { get; }

        public int Vnodes { get; }
    }

    internal sealed class AdminOwnerLookupSample
    {
        public AdminOwnerLookupSample(string key, string owner)
        {
            Key = key;
            Owner = owner;
        }

        public string Key { get; }

        public string Owner { get; }
    }

    internal sealed class AdminOwnerResponse
    {
        public AdminOwnerResponse(string key, string owner)
        {
            Key = key;
            Owner = owner;
        }

        public string Key { get; }

        public string Owner { get; }
    }

    internal sealed class AdminRebalanceHistoryEvent
    {
        public AdminRebalanceHistoryEvent(
            long sequence,
            DateTime timestampUtc,
            string action,
            string? nodeId,
            string[] previousMembers,
            string[] currentMembers,
            int previousVirtualNodes,
            int currentVirtualNodes)
        {
            Sequence = sequence;
            TimestampUtc = timestampUtc;
            Action = action;
            NodeId = nodeId;
            PreviousMembers = previousMembers;
            CurrentMembers = currentMembers;
            PreviousVirtualNodes = previousVirtualNodes;
            CurrentVirtualNodes = currentVirtualNodes;
        }

        public string Action { get; }

        public string[] CurrentMembers { get; }

        public int CurrentVirtualNodes { get; }

        public string? NodeId { get; }

        public string[] PreviousMembers { get; }

        public int PreviousVirtualNodes { get; }

        public long Sequence { get; }

        public DateTime TimestampUtc { get; }
    }

    internal sealed class AdminRebalanceHistoryResponse
    {
        public AdminRebalanceHistoryResponse(int retention, AdminRebalanceHistoryEvent[] events)
        {
            Retention = retention;
            Events = events;
        }

        public AdminRebalanceHistoryEvent[] Events { get; }

        public int Retention { get; }
    }

    internal sealed class AdminRingNodeDistribution
    {
        public AdminRingNodeDistribution(string nodeId, int sampleKeys, double sampleShare, int configuredVirtualNodes)
        {
            NodeId = nodeId;
            SampleKeys = sampleKeys;
            SampleShare = sampleShare;
            ConfiguredVirtualNodes = configuredVirtualNodes;
        }

        public int ConfiguredVirtualNodes { get; }

        public string NodeId { get; }

        public int SampleKeys { get; }

        public double SampleShare { get; }
    }

    internal sealed class AdminRingResponse
    {
        public AdminRingResponse(
            int virtualNodes,
            string[] members,
            int sampleSize,
            IReadOnlyList<AdminRingNodeDistribution> vnodeDistribution,
            IReadOnlyList<AdminOwnerLookupSample> ownerLookupSamples)
        {
            VirtualNodes = virtualNodes;
            Members = members;
            SampleSize = sampleSize;
            VnodeDistribution = vnodeDistribution;
            OwnerLookupSamples = ownerLookupSamples;
        }

        public string[] Members { get; }

        public IReadOnlyList<AdminOwnerLookupSample> OwnerLookupSamples { get; }

        public int SampleSize { get; }

        public int VirtualNodes { get; }

        public IReadOnlyList<AdminRingNodeDistribution> VnodeDistribution { get; }
    }

    internal sealed class AdminStorageDiagnosticsResponse
    {
        public AdminStorageDiagnosticsResponse(string dataDir, AdminManifestSnapshot manifest, AdminJournalWriterDiagnostics writer, AdminJournalDiagnostics journal)
        {
            DataDir = dataDir;
            Manifest = manifest;
            Writer = writer;
            Journal = journal;
        }

        public string DataDir { get; }

        public AdminManifestSnapshot Manifest { get; }

        [JsonPropertyName("journal")]
        public AdminJournalDiagnostics Journal { get; }

        public AdminJournalWriterDiagnostics Writer { get; }
    }

    internal sealed class AdminUnsupportedMutationResponse
    {
        public AdminUnsupportedMutationResponse(string error)
        {
            Error = error;
        }

        public string Error { get; }
    }

    internal sealed class AdminJournalDiagnostics
    {
        public AdminJournalDiagnostics(int recentSegmentLimit, AdminJournalSegmentDiagnostic[] segments)
        {
            RecentSegmentLimit = recentSegmentLimit;
            Segments = segments;
        }

        public int RecentSegmentLimit { get; }

        public AdminJournalSegmentDiagnostic[] Segments { get; }
    }

    internal sealed class AdminJournalSegmentDiagnostic
    {
        public AdminJournalSegmentDiagnostic(int index, string path, string fileName, bool exists, long lengthBytes, DateTime? lastWriteUtc, bool headerValid, string? error)
        {
            Index = index;
            Path = path;
            FileName = fileName;
            Exists = exists;
            LengthBytes = lengthBytes;
            LastWriteUtc = lastWriteUtc;
            HeaderValid = headerValid;
            Error = error;
        }

        public string? Error { get; }

        public bool Exists { get; }

        public string FileName { get; }

        public bool HeaderValid { get; }

        public int Index { get; }

        public DateTime? LastWriteUtc { get; }

        public long LengthBytes { get; }

        public string Path { get; }
    }

    internal sealed class AdminJournalWriterDiagnostics
    {
        public AdminJournalWriterDiagnostics(int currentJournal, ulong nextSequence, long appendedOps, long appendedBytes, double recentAppendLatencyMs)
        {
            CurrentJournal = currentJournal;
            NextSequence = nextSequence;
            AppendedOps = appendedOps;
            AppendedBytes = appendedBytes;
            RecentAppendLatencyMs = recentAppendLatencyMs;
        }

        public long AppendedBytes { get; }

        public long AppendedOps { get; }

        [JsonPropertyName("currentJournal")]
        public int CurrentJournal { get; }

        public ulong NextSequence { get; }

        public double RecentAppendLatencyMs { get; }
    }

    internal sealed class AdminWhoamiResponse
    {
        public AdminWhoamiResponse(string nodeId, string url)
        {
            NodeId = nodeId;
            Url = url;
        }

        public string NodeId { get; }

        public string Url { get; }
    }

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

        public HealthMemoryPressureDetails MemoryPressure { get; }

        public double? SnapshotAgeSeconds { get; }

        public bool SnapshotInFlight { get; }

        [JsonPropertyName("journalBacklogOps")]
        public long JournalBacklogOps { get; }
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
