using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit.Hosting;

namespace Squirix.Server.TestKit.Benchmarks;

/// <summary>Exposes the real distributed owner access for a transport breakdown without exposing server contracts.</summary>
public sealed class BenchmarkDistributedReadSurface
{
    private readonly IDistributedCacheAccess _access;
    private readonly string _cacheName;
    private readonly RpcMutationIdempotencyStore _outcomes;

    private BenchmarkDistributedReadSurface(ITestNodeHost host, string cacheName)
    {
        _access = host.GetRequiredService<IDistributedCacheAccess>();
        _outcomes = host.GetRequiredService<RpcMutationIdempotencyStore>();
        _cacheName = cacheName;
    }

    /// <summary>Resolves distributed access from a running single-copy node.</summary>
    /// <param name="host">The running host.</param>
    /// <param name="cacheName">The namespace used by every measured layer.</param>
    /// <returns>The benchmark access surface.</returns>
    public static BenchmarkDistributedReadSurface ForCache(ITestNodeHost host, string cacheName)
    {
        ArgumentNullException.ThrowIfNull(host);
        ArgumentException.ThrowIfNullOrWhiteSpace(cacheName);
        return new BenchmarkDistributedReadSurface(host, cacheName);
    }

    /// <summary>Runs the real access including admission, probes and response copies, excluding routing, interceptors and transport.</summary>
    /// <param name="key">The seeded key.</param>
    /// <param name="operationId">A fresh logical operation identity.</param>
    /// <param name="cancellationToken">The caller token.</param>
    /// <returns>A fresh owned payload array, or null for a miss.</returns>
    public async Task<byte[]?> GetAsync(string key, string operationId, CancellationToken cancellationToken)
    {
        var reply = await _access.AccessAsync(operationId, _cacheName, key, RpcMutationFingerprints.DistributedAccess, false, cancellationToken).ConfigureAwait(false);
        return reply.Found ? reply.Value.ToByteArray() : null;
    }

    /// <summary>Adds completed outcomes through the normal recording path, outside measured reads.</summary>
    /// <param name="count">The number of extra live outcomes.</param>
    public void SeedOutcomes(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        for (var index = 0; index < count; index++)
            _outcomes.RecordSuccess(string.Create(CultureInfo.InvariantCulture, $"transport-prefill-{index:D8}"), "fingerprint", [], null);
    }
}
