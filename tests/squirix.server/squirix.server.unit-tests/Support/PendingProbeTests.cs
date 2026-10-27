using System.Threading.Tasks;
using Squirix.Server.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Guards the shared pending probe the stalled-gate tests rely on.</summary>
[Immutable]
public sealed class PendingProbeTests
{
    /// <summary>An operation nobody completes is reported pending.</summary>
    [Test]
    public async Task UncompletedOperationStaysPending()
    {
        var operation = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        _ = await Assert.That(await PendingProbe.StaysPendingAsync(operation.Task)).IsTrue();
    }

    /// <summary>A completed operation is reported completed at once.</summary>
    [Test]
    public async Task CompletedOperationIsNotPending() => _ = await Assert.That(await PendingProbe.StaysPendingAsync(Task.CompletedTask)).IsFalse();
}
