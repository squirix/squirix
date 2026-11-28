using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Node.Observability;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Codec;

namespace Squirix.Server.Cluster;

internal static class PeerDeadline
{
    internal static Task<T> SendAsync<T>(IPeerClient client, PeerOperation operation, WireMessage request, WireParser<T> parser, ServerDeadlineBudget? budget, CancellationToken cancellationToken)
        where T : WireMessage =>
        client is IPeerDeadlineClient timed
            ? timed.SendWithDeadlineAsync(operation, request, parser, budget, cancellationToken)
            : client.SendAsync(operation, request, parser, cancellationToken);

    internal static long? RemainingBudget(ServerDeadlineBudget? deadlineBudget)
    {
        var ticks = deadlineBudget?.Remaining.Ticks;
        return ticks <= 0 ? throw new ServerCallException(FrameStatus.DeadlineExceeded, "Request deadline exceeded.") : ticks;
    }
}
