using Squirix.Attributes;
using Squirix.Internal.Cluster.Reliability;

namespace Squirix.Internal.Cluster.Transport;

/// <summary>The transport client and execution policy registered for one node.</summary>
/// <param name="Client">The node's reusable binary client.</param>
/// <param name="Policy">The policy governing calls to that node.</param>
[Immutable]
internal readonly record struct NodeClient(BinaryCacheClient Client, ICallPolicy Policy);
