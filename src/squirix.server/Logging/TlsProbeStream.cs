using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Logging;

/// <summary>Reports the network I/O awaited by a TLS handshake while a diagnostic observer is attached.</summary>
internal class TlsProbeStream : NetworkStream
{
    private Action<string>? _report;

    internal TlsProbeStream(Socket socket, bool ownsSocket, Action<string>? report)
        : base(socket, ownsSocket)
    {
        _report = report;
    }

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
        _report is { } report ? ReadTrackedAsync(buffer, report, cancellationToken) : base.ReadAsync(buffer, cancellationToken);

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) =>
        _report is { } report ? WriteTrackedAsync(buffer, report, cancellationToken) : base.WriteAsync(buffer, cancellationToken);

    internal void StopTracing() => _report = null;

    private async ValueTask<int> ReadTrackedAsync(Memory<byte> buffer, Action<string> report, CancellationToken cancellationToken)
    {
        report("read pending");
        var count = await base.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
        report($"read done bytes={count}");
        return count;
    }

    private async ValueTask WriteTrackedAsync(ReadOnlyMemory<byte> buffer, Action<string> report, CancellationToken cancellationToken)
    {
        report($"write pending bytes={buffer.Length}");
        await base.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
        report($"write done bytes={buffer.Length}");
    }
}
