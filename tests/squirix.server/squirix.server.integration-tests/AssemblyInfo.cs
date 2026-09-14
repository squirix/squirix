using Rocks;
using Squirix.Server.Cluster.Replication;

[assembly: Rock(typeof(IReplicaCommitFaultHooks), BuildType.Create)]
[assembly: Rock(typeof(IReplicaCommitPipeline), BuildType.Create)]
