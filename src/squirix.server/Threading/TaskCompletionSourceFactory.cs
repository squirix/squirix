using System.Threading.Tasks;

namespace Squirix.Server.Threading;

/// <summary>Creates <see cref="TaskCompletionSource"/> instances configured to run continuations asynchronously.</summary>
/// <remarks>Centralizes the <see cref="TaskCreationOptions.RunContinuationsAsynchronously"/> flag used across the server.</remarks>
internal static class TaskCompletionSourceFactory
{
    internal static TaskCompletionSource Create() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
