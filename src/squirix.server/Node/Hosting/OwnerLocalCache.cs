using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.Hosting;

internal sealed record OwnerLocalCache(ILogicalNamespacedCache<StoredValue> Value);
