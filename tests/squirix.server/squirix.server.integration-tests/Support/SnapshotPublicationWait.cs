using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit;

namespace Squirix.Server.IntegrationTests.Support;

internal static class SnapshotPublicationWait
{
    internal static Task WaitAsync(Ledger ledger, Coordinator coordinator, int previousIndex, TimeSpan timeout, CancellationToken cancellationToken) =>
        (Ledger: ledger, Coordinator: coordinator, PreviousIndex: previousIndex).WaitUntilValueAsync(
            static (state, token) => HasPublishedAsync(state.Ledger, state.Coordinator, state.PreviousIndex, token),
            timeout,
            cancellationToken);

    internal static async ValueTask<bool> HasPublishedAsync(Ledger ledger, Coordinator coordinator, int previousIndex, CancellationToken cancellationToken)
    {
        var manifest = await ledger.ReadCurrentOrDefaultAsync(cancellationToken);
        return manifest.LastSnapshot?.Index > previousIndex && !coordinator.IsInFlight;
    }
}
