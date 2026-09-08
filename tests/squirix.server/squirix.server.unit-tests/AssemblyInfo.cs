using Microsoft.AspNetCore.Http;
using Rocks;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Storage.Snapshot;

[assembly: Rock(typeof(INodeLocator), BuildType.Create)]
[assembly: Rock(typeof(IMemoryBudgetProvider), BuildType.Create)]
[assembly: Rock(typeof(IReplicaCommitFaultHooks), BuildType.Create)]
[assembly: Rock(typeof(IReplicaCommitPipeline), BuildType.Create)]
[assembly: Rock(typeof(IBackpressureClientIdResolver), BuildType.Create)]
[assembly: Rock(typeof(IBackpressureGate), BuildType.Create)]
[assembly: Rock(typeof(ILogicalNamespacedCache<>), BuildType.Create)]
[assembly: Rock(typeof(ISquirixServerEntryCachePipeline<>), BuildType.Create)]
[assembly: Rock(typeof(IJournalCoordinator), BuildType.Create)]
[assembly: Rock(typeof(IFollowerLogFaultHooks), BuildType.Create)]
[assembly: Rock(typeof(IServerClientPool), BuildType.Create)]
[assembly: Rock(typeof(IServerCallPolicy), BuildType.Create)]
[assembly: Rock(typeof(ILocalCacheStats), BuildType.Create)]
[assembly: Rock(typeof(IServerSerializer), BuildType.Create)]
[assembly: Rock(typeof(IJournalOperationTracer), BuildType.Create)]
[assembly: Rock(typeof(IJournalOperationTraceScope), BuildType.Create)]
[assembly: Rock(typeof(IExclusiveMaintenanceExecutor), BuildType.Create)]
[assembly: Rock(typeof(IStorageFileOperations), BuildType.Create)]
[assembly: Rock(typeof(IBackgroundSnapshotMemoryThrottle), BuildType.Create)]
[assembly: Rock(typeof(ISnapshotEntryCapture), BuildType.Create)]
[assembly: Rock(typeof(IIdempotencySnapshotExporter), BuildType.Create)]
[assembly: Rock(typeof(ISnapshotWriter), BuildType.Create)]
[assembly: Rock(typeof(IManifestRetentionFailureMetrics), BuildType.Create)]
[assembly: Rock(typeof(IRetentionCleanupReadinessStatus), BuildType.Create)]
[assembly: Rock(typeof(IHttpContextAccessor), BuildType.Create)]
