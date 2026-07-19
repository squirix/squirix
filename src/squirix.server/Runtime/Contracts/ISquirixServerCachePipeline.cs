using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>Basic logical cache pipeline surface available to integrations.</summary>
public interface ISquirixServerCachePipeline
{
    /// <summary>Updates expiration for an entry.</summary>
    /// <param name="operationId">Client mutation id for idempotent RPC replay.</param>
    /// <param name="cacheName">Cache name.</param>
    /// <param name="key">Cache key.</param>
    /// <param name="expiration">New time to live.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True when the key was found.</returns>
    ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken);
}
