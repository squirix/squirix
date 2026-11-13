using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Extensions.Caching;

/// <summary>Invokes synchronous adapter operations without capturing a caller's context or allocating closures.</summary>
internal static class DistributedCacheSync
{
    private static bool CanInvokeInline => SynchronizationContext.Current == null && TaskScheduler.Current == TaskScheduler.Default;

    [SuppressMessage("Usage", "VSTHRD002", Justification = "The synchronous interface blocks on context-independent work.")]
    [SuppressMessage("Usage", "MA0045", Justification = "IDistributedCache exposes a synchronous API.")]
    internal static TResult Run<TState, TResult>(TState state, Func<TState, Task<TResult>> operation) =>
        (CanInvokeInline ? operation(state) : InvokeAsync(state, operation)).GetAwaiter().GetResult();

    [SuppressMessage("Usage", "VSTHRD002", Justification = "The synchronous interface blocks on context-independent work.")]
    [SuppressMessage("Usage", "MA0045", Justification = "IDistributedCache and IDisposable expose synchronous APIs.")]
    internal static void Run<TState>(TState state, Func<TState, Task> operation) =>
        (CanInvokeInline ? operation(state) : InvokeAsync(state, operation)).GetAwaiter().GetResult();

    private static async Task<TResult> InvokeAsync<TState, TResult>(TState state, Func<TState, Task<TResult>> operation)
    {
        await Task.CompletedTask.ConfigureAwait(ConfigureAwaitOptions.ForceYielding);
        return await operation(state).ConfigureAwait(false);
    }

    private static async Task InvokeAsync<TState>(TState state, Func<TState, Task> operation)
    {
        await Task.CompletedTask.ConfigureAwait(ConfigureAwaitOptions.ForceYielding);
        await operation(state).ConfigureAwait(false);
    }
}
