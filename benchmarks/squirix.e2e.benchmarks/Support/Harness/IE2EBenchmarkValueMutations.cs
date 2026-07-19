using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.E2EBenchmarks.Support.Harness;

/// <summary>Mutation-path cache operations for a benchmark value shape.</summary>
internal interface IE2EBenchmarkValueMutations
{
    Task AddAsync(string key, int valueIndex, CancellationToken cancellationToken);

    Task<bool> AddConflictAsync(string key, int valueIndex, CancellationToken cancellationToken);

    Task<bool> RemoveAsync(string key, CancellationToken cancellationToken);

    Task<bool> RemoveExpirationAsync(string key, CancellationToken cancellationToken);

    Task SetAsync(string key, int valueIndex, CancellationToken cancellationToken);

    Task SetExpiringAsync(string key, int valueIndex, TimeSpan expiration, CancellationToken cancellationToken);

    Task<bool> TouchAbsoluteAsync(string key, DateTimeOffset expiresAt, CancellationToken cancellationToken);

    Task<bool> TouchRelativeAsync(string key, TimeSpan expiration, CancellationToken cancellationToken);

    Task<bool> TryAddAsync(string key, int valueIndex, CancellationToken cancellationToken);

    Task<bool> UpdateAsync(string key, int valueIndex, CancellationToken cancellationToken);
}
