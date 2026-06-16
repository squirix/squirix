using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Host;

internal sealed class ShutdownSignal : IAsyncDisposable
{
    private readonly CancellationTokenSource _cts = new();

    public ShutdownSignal()
    {
        Console.CancelKeyPress += OnCancelKeyPress;
    }

    public CancellationToken Token => _cts.Token;

    public async ValueTask DisposeAsync()
    {
        Console.CancelKeyPress -= OnCancelKeyPress;
        await _cts.CancelAsync().ConfigureAwait(false);
        _cts.Dispose();
    }

    private void OnCancelKeyPress(object? sender, ConsoleCancelEventArgs e)
    {
        e.Cancel = true;
        _ = _cts.CancelAsync();
    }
}
