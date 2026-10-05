using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Drives a <see cref="FakeTimeProvider" /> for tests whose timers are armed by code running on other threads.</summary>
internal static class FakeClockKit
{
    /// <summary>Advances <paramref name="clock" /> in small steps until <paramref name="pending" /> completes, so a timer armed late is still reached.</summary>
    /// <typeparam name="T">The result type of the pending operation.</typeparam>
    /// <param name="clock">The fake clock the operation's timers run on.</param>
    /// <param name="pending">The started operation.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The operation's result; a failure of the operation is rethrown.</returns>
    internal static async ValueTask<T> AdvanceUntilCompletedAsync<T>(FakeTimeProvider clock, ValueTask<T> pending, CancellationToken cancellationToken)
    {
        var task = pending.AsTask();
        while (!task.IsCompleted)
        {
            cancellationToken.ThrowIfCancellationRequested();
            clock.Advance(TimeSpan.FromMilliseconds(10));
            await Task.Yield();
        }

        return await task;
    }
}
