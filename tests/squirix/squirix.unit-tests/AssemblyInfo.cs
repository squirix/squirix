using Rocks;
using Squirix.Internal.Cluster.Transport;

[assembly: Rock(typeof(IClientPool), BuildType.Create)]
[assembly: Rock(typeof(Squirix.ISquirixSerializer), BuildType.Create)]
