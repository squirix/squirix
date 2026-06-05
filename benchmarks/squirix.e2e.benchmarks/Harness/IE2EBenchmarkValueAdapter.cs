using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.E2EBenchmarks.Harness;

/// <summary>
/// Non-generic adapter over typed cache operations for a benchmark value shape.
/// </summary>
internal interface IE2EBenchmarkValueAdapter
{
    Task AddAsync(string key, int valueIndex, CancellationToken cancellationToken);

    Task<bool> AddConflictAsync(string key, int valueIndex, CancellationToken cancellationToken);

    Task<bool> GetEntryHitAsync(string key, CancellationToken cancellationToken);

    Task<bool> GetExpirationAsync(string key, CancellationToken cancellationToken);

    Task<bool> GetOrAddHitAsync(string key, CancellationToken cancellationToken);

    Task<bool> GetOrAddMissAsync(string key, int valueIndex, CancellationToken cancellationToken);

    Task<bool> GetValueHitAsync(string key, CancellationToken cancellationToken);

    Task<bool> GetValueMissAsync(string key, CancellationToken cancellationToken);

    Task<bool> RemoveAsync(string key, CancellationToken cancellationToken);

    Task<bool> RemoveExpirationAsync(string key, CancellationToken cancellationToken);

    Task SeedAsync(string[] keys, CancellationToken cancellationToken);

    Task SeedExpiringAsync(string[] keys, TimeSpan expiration, CancellationToken cancellationToken);

    Task SetAsync(string key, int valueIndex, CancellationToken cancellationToken);

    Task SetExpiringAsync(string key, int valueIndex, TimeSpan expiration, CancellationToken cancellationToken);

    Task<bool> TouchAbsoluteAsync(string key, DateTimeOffset expiresAt, CancellationToken cancellationToken);

    Task<bool> TouchRelativeAsync(string key, TimeSpan expiration, CancellationToken cancellationToken);

    Task<bool> TryAddAsync(string key, int valueIndex, CancellationToken cancellationToken);

    Task<bool> UpdateAsync(string key, int valueIndex, CancellationToken cancellationToken);
}
