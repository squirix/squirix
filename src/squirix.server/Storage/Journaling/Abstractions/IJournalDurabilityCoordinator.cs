using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Threading;

namespace Squirix.Server.Storage.Journaling.Abstractions;

/// <summary>Durability flush and pending in-memory apply coordination for journal-backed mutations.</summary>
internal interface IJournalDurabilityCoordinator
{
    IQuiescenceGate InFlightApplyGate { get; }

    ValueTask AwaitDurabilityCommitAsync(CancellationToken cancellationToken);
}
