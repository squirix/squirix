using System.Threading;
using System.Threading.Tasks;

namespace Squirix.E2EBenchmarks.Support.Harness;

/// <summary>Read-path cache operations for a benchmark value shape.</summary>
internal interface IE2EBenchmarkValueReads
{
    Task<bool> GetEntryHitAsync(string key, CancellationToken cancellationToken);

    Task<bool> GetExpirationAsync(string key, CancellationToken cancellationToken);

    Task<bool> GetOrAddHitAsync(string key, CancellationToken cancellationToken);

    Task<bool> GetOrAddMissAsync(string key, int valueIndex, CancellationToken cancellationToken);

    Task<bool> GetValueHitAsync(string key, CancellationToken cancellationToken);

    Task<bool> GetValueMissAsync(string key, CancellationToken cancellationToken);
}
