using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>Safety tests for <see cref="Ledger.WriteAsync" /> against the on-disk manifest slots.</summary>
[Immutable]
public sealed class StoreWriteSafetyTests : IsolatedStorageTestBase
{
    /// <summary>Verifies each write moves the slot sequence forward by a publish and its mirror.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WriteAdvancesSlotSequence(CancellationToken cancellationToken)
    {
        var options = StoreTestSupport.CreateOptions(Dir);
        using var store = new Ledger(options, NullLogger<Ledger>.Instance);
        await store.WriteAsync(new State { CurrentJournal = 1 }, cancellationToken);

        var first = StoreTestSupport.ReadCurrentSelection(Dir);
        _ = await Assert.That(first.State.CurrentJournal).IsEqualTo(1);
        _ = await Assert.That(first.Sequence).IsEqualTo(2UL);

        await store.WriteAsync(new State { CurrentJournal = 2 }, cancellationToken);

        var second = StoreTestSupport.ReadCurrentSelection(Dir);
        _ = await Assert.That(second.State.CurrentJournal).IsEqualTo(2);
        _ = await Assert.That(second.Sequence).IsEqualTo(4UL);
    }
}
