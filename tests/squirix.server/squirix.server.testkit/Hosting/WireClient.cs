using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Sends <see cref="WireMutation" /> instances to one node over the public gRPC contract, without the SDK.</summary>
[Immutable]
internal sealed class WireClient : IAsyncDisposable
{
    private readonly GrpcChannel _channel;
    private readonly SquirixCacheService.SquirixCacheServiceClient _client;

    private WireClient(GrpcChannel channel)
    {
        _channel = channel;
        _client = new SquirixCacheService.SquirixCacheServiceClient(channel);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _channel.Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>Opens a client to one node.</summary>
    /// <param name="uri">The address of the node.</param>
    /// <returns>The client.</returns>
    internal static WireClient Connect(Uri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);
        return new WireClient(GrpcChannel.ForAddress(uri, new GrpcChannelOptions { HttpHandler = LoopbackHttp.CreateHandler(), DisposeHttpClient = true }));
    }

    /// <summary>Sends a mutation once, with the operation identifier it was built with.</summary>
    /// <param name="mutation">The mutation.</param>
    /// <param name="deadline">The longest time the call may take.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task whose result is the answer of the node; it fails with the status of the call.</returns>
    internal Task<WireOutcome> SendAsync(WireMutation mutation, TimeSpan deadline, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(mutation);
        return mutation.SendAsync(_client, DateTime.UtcNow.Add(deadline), cancellationToken);
    }
}
