using System;

namespace Squirix.Server.Node.Backpressure;

internal readonly struct BackpressureLease : IDisposable
{
    private readonly Action? _release;

    public BackpressureLease(Action? release)
    {
        _release = release;
    }

    public static BackpressureLease Empty => default;

    public void Dispose() => _release?.Invoke();
}
