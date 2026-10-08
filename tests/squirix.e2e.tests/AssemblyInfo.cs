using Rocks;
using Squirix;
using Squirix.Server.TestKit;
using TUnit.Core;

[assembly: ParallelLimiter<ServerProcessorCountLimit>]
[assembly: Rock(typeof(ICache<>), BuildType.Create)]
