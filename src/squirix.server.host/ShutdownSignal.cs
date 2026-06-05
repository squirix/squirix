using System;
using System.Threading;

namespace Squirix.Server.Host;

internal sealed class ShutdownSignal : IDisposable
{
    private readonly CancellationTokenSource _cts = new();

    public ShutdownSignal()
    {
        Console.CancelKeyPress += OnCancelKeyPress;
    }

    public CancellationToken Token => _cts.Token;

    public void Dispose()
    {
        Console.CancelKeyPress -= OnCancelKeyPress;
        _cts.Dispose();
    }

    private void OnCancelKeyPress(object? sender, ConsoleCancelEventArgs e)
    {
        e.Cancel = true;
        _cts.Cancel();
    }
}
