using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Journaling.Abstractions;

/// <summary>Durability flush and pending in-memory apply coordination for journal-backed mutations.</summary>
internal interface IJournalDurabilityCoordinator
{
    ValueTask AwaitDurabilityCommitAsync(CancellationToken cancellationToken);

    void BeginPendingMemoryApply();

    void CompletePendingMemoryApply();
}
