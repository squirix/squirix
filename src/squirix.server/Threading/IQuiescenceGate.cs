using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Threading;

/// <summary>
/// Gate that tracks in-flight operations and blocks until they all complete, establishing a quiet
/// point before a barrier (such as a snapshot or reclamation) can proceed.
/// </summary>
internal interface IQuiescenceGate
{
    bool HasPending { get; }

    void Enter();

    void Exit();

    ValueTask WaitAsync(CancellationToken cancellationToken);
}
