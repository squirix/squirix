using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Node.Endpoint;

/// <summary>
/// journal compaction trigger surface for admin REST endpoints.
/// </summary>
internal sealed class AdminJournalCompactionTrigger : IAdminJournalCompactionTrigger
{
    private readonly JournalCompactionController _controller;

    public AdminJournalCompactionTrigger(JournalCompactionController controller)
    {
        _controller = controller ?? throw new ArgumentNullException(nameof(controller));
    }

    /// <inheritdoc />
    public async ValueTask<bool> TryTriggerCompactionAsync(CancellationToken cancellationToken) => await _controller.TryTriggerNowAsync(cancellationToken).ConfigureAwait(false);
}
