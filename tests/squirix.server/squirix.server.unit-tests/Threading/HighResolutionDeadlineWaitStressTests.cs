using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Threading;

/// <summary>Timing check for the high-resolution deadline wait, marked as a stress test because it depends on scheduler load.</summary>
[Immutable]
[Property(StressTrait.TraitName, StressTrait.TraitValue)]
public sealed class HighResolutionDeadlineWaitStressTests
{
    private const int Samples = 20;

    /// <summary>A short wait on a supported Windows resolves far below the system clock tick.</summary>
    [Test]
    public async Task ShortWaitResolvesBelowClockTick()
    {
        if (!OperatingSystem.IsWindowsVersionAtLeast(10, 0, 17134))
            return;

        using var signal = new AutoResetEvent(false);
        using var wait = new HighResolutionDeadlineWait(signal, true);
        var elapsedMs = new double[Samples];
        for (var i = 0; i < Samples; i++)
        {
            var started = Stopwatch.GetTimestamp();
            wait.Wait(2);
            elapsedMs[i] = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        }

        Array.Sort(elapsedMs);
        _ = await Assert.That(elapsedMs[Samples / 2]).IsLessThan(8d);
    }
}
