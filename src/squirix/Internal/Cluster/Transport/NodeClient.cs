using Squirix.Attributes;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Internal.Cluster.Transport;

/// <summary>The transport client and execution policy registered for one node.</summary>
/// <param name="Client">The node's reusable gRPC client.</param>
/// <param name="Policy">The policy governing calls to that node.</param>
[Immutable]
internal readonly record struct NodeClient(SquirixCacheService.SquirixCacheServiceClient Client, ICallPolicy Policy);
