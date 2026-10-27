using Rocks;
using Squirix;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Squirix.TestKit;
using TUnit.Core;

[assembly: Rock(typeof(ICallPolicy), BuildType.Create)]
[assembly: Rock(typeof(IClientPool), BuildType.Create)]
[assembly: Rock(typeof(ISquirixSerializer), BuildType.Create)]
[assembly: ParallelLimiter<ClientProcessorCountLimit>]
