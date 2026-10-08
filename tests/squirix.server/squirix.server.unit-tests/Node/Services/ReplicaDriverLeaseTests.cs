using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The applier lease excludes only a leadership: passes share the applier, and a leadership waits for them and keeps them out.</summary>
public sealed class ReplicaDriverLeaseTests : ServerUnitTestBase
{
    /// <summary>
    /// Without a leadership two passes run at once; a leadership waits until both ended, refuses new passes while it lasts, and lets them in
    /// again once it ends.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeadershipExcludesPassesOnly(CancellationToken cancellationToken)
    {
        var lease = new ReplicaDriverLease();
        var first = lease.TryEnterPass();
        var second = lease.TryEnterPass();

        var leading = lease.LeadAsync(cancellationToken);
        var refusedWhileWaiting = !lease.TryEnterPass();
        lease.ExitPass();
        var waitedForSecond = !leading.IsCompleted;
        lease.ExitPass();
        await leading;
        var refusedWhileLeading = !lease.TryEnterPass();
        lease.EndLeading();
        var admittedAfter = lease.TryEnterPass();
        lease.ExitPass();

        _ = await Assert.That((first, second, refusedWhileWaiting, waitedForSecond, refusedWhileLeading, admittedAfter)).IsEqualTo((true, true, true, true, true, true));
    }

    /// <summary>A leadership whose wait is canceled does not start: passes enter again.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledLeadershipLetsPassesIn(CancellationToken cancellationToken)
    {
        var lease = new ReplicaDriverLease();
        _ = lease.TryEnterPass();
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var leading = lease.LeadAsync(cancel.Token);

        await cancel.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(leading);
        lease.ExitPass();

        _ = await Assert.That((lease.IsLeading, lease.TryEnterPass())).IsEqualTo((false, true));
    }
}
