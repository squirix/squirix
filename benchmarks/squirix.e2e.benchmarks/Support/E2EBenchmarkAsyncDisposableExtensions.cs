using System;
using System.Threading.Tasks;

namespace Squirix.E2EBenchmarks.Support;

/// <summary>Observes async disposal failures without letting them escape.</summary>
/// <remarks>
/// Benchmark-only capture: the benchmark assemblies have no access to server internals, so the single broad catch
/// lives here and call sites stay free of general catch blocks. It does not report the failure itself: the caller
/// decides whether to log or trace it.
/// </remarks>
internal static class E2EBenchmarkAsyncDisposableExtensions
{
    /// <summary>Disposes <paramref name="disposable" /> and returns any failure instead of throwing it.</summary>
    /// <param name="disposable">The object to dispose.</param>
    /// <returns>The failure, or <see langword="null" /> when the disposal completed.</returns>
    internal static async ValueTask<Exception?> CaptureDisposeFailureAsync(this IAsyncDisposable disposable)
    {
        ArgumentNullException.ThrowIfNull(disposable);
        try
        {
            await disposable.DisposeAsync().ConfigureAwait(false);
            return null;
        }
        catch (Exception exception)
        {
            return exception;
        }
    }
}
