using Rocks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using TUnit.Core;

[assembly: Rock(typeof(IReplicaCommitFaultHooks), BuildType.Create)]
[assembly: Rock(typeof(IReplicaCommitPipeline), BuildType.Create)]
[assembly: Rock(typeof(IJournalCoordinator), BuildType.Create)]
[assembly: Rock(typeof(IFollowerLogFaultHooks), BuildType.Create)]
[assembly: ParallelLimiter<ServerProcessorCountLimit>]
