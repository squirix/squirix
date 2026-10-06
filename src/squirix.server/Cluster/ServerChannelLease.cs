using System;
using System.Threading;
using Grpc.Net.Client;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;

namespace Squirix.Server.Cluster;

/// <summary>One outbound call's lease on a pooled channel; the pool drains every lease before it disposes the channel.</summary>
/// <remarks>Dispose the lease exactly once, with <see langword="using" />, after the call completed. Issue calls with <see cref="Token" />, which the pool cancels when it starts to dispose.</remarks>
[Immutable]
internal readonly record struct ServerChannelLease : IDisposable
{
    private readonly CancellationTokenSource? _cancellation;

    private readonly QuiescenceGate? _calls;

    internal ServerChannelLease(GrpcChannel channel, CancellationTokenSource cancellation, QuiescenceGate calls)
    {
        Channel = channel;
        _cancellation = cancellation;
        _calls = calls;
    }

    /// <summary>Gets the pooled channel.</summary>
    internal GrpcChannel Channel { get; }

    /// <summary>Gets the caller's token linked with the pool's disposal.</summary>
    internal CancellationToken Token => _cancellation?.Token ?? CancellationToken.None;

    /// <summary>Ends the lease: the pool may dispose the channel once every lease ended. A default lease does nothing.</summary>
    public void Dispose()
    {
        if (_calls == null)
            return;

        _cancellation?.Dispose();
        _calls.Exit();
    }
}
