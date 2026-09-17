using System;
using System.Threading;

namespace Squirix.Server.TestKit.Networking;

/// <summary>A loopback listen port held bound until released for the real bind.</summary>
/// <remarks>
/// The pool owns the underlying reservation; this handle only proves that the port was
/// taken through <see cref="ListenPortPool.HoldPort" /> or <see cref="ListenPortPool.HoldPorts" />
/// rather than probed. Releasing (or disposing) unbinds the hold so the real server can bind;
/// the port stays reserved in-process and is never reissued.
/// </remarks>
public sealed class HeldPort : IDisposable
{
    private readonly ListenPortPool _pool;
    private int _released;

    /// <summary>Initializes a new instance of the <see cref="HeldPort" /> class.</summary>
    /// <param name="pool">The pool owning the reservation.</param>
    /// <param name="port">The held loopback port number.</param>
    /// <param name="httpUri">A loopback HTTPS listen URI for the held port.</param>
    internal HeldPort(ListenPortPool pool, int port, Uri httpUri)
    {
        _pool = pool;
        Port = port;
        HttpUri = httpUri;
    }

    /// <summary>Gets the held loopback port number.</summary>
    public int Port { get; }

    /// <summary>Gets a loopback HTTPS listen URI for the held port.</summary>
    public Uri HttpUri { get; }

    /// <summary>Gets a value indicating whether the hold has been released for the real bind.</summary>
    internal bool IsReleased => Volatile.Read(ref _released) != 0;

    /// <summary>Releases the hold so the real server can bind the port. Safe to call multiple times.</summary>
    public void Dispose() => ReleaseHold();

    private void ReleaseHold()
    {
        if (Interlocked.Exchange(ref _released, 1) == 0)
            _pool.ReleasePort(Port);
    }
}
