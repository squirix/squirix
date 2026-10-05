using System;
using System.Threading.Tasks;

namespace Squirix.Server.UnitTests.Support;

/// <summary>
/// Checks that an operation stays parked on a gate the test holds, without a wall-clock window: the check lets a fixed number of
/// thread pool turns run, so work already queued behind the operation (continuations of a wait that honored a cancellation, a gate
/// that did not hold) completes first, then reads the state synchronously.
/// </summary>
internal static class PendingProbe
{
    private const int SettleTurns = 128;

    /// <summary>Determines whether <paramref name="operation" /> is still incomplete after the queued continuations had their turns.</summary>
    /// <param name="operation">The operation expected to be parked.</param>
    /// <returns><see langword="true" /> when the operation has not completed; otherwise <see langword="false" />.</returns>
    internal static async ValueTask<bool> StaysPendingAsync(Task operation)
    {
        ArgumentNullException.ThrowIfNull(operation);

        for (var turn = 0; turn < SettleTurns && !operation.IsCompleted; turn++)
            await Task.Yield();

        return !operation.IsCompleted;
    }
}
