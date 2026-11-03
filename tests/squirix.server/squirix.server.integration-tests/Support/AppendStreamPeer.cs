using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Observability;
using Squirix.Server.Utils;

namespace Squirix.Server.IntegrationTests.Support;

/// <summary>A real HTTP/2 peer with scripted replies, used to exercise client stream ownership and framing.</summary>
internal sealed class AppendStreamPeer : SquirixReplicationService.SquirixReplicationServiceBase, IAsyncDisposable
{
    private readonly Meter _meter = new("AppendStreamTests");
    private WebApplication? _app;
    private ServerClientPool? _pool;
    private ReplicaAppendStream? _transport;
    private int _frames;
    private int _streams;
    private int _unary;

    internal TaskCompletionSource Arrived { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal TaskCompletionSource Ended { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal TaskCompletionSource ResumeAfterReply { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal int AfterReplyKind { get; set; }

    internal bool BlockFirst { get; set; }

    internal int ResponseKind { get; set; }

    internal StatusCode? Failure { get; set; }

    internal int Streams => Volatile.Read(ref _streams);

    internal int Unary => Volatile.Read(ref _unary);

    internal ServerClientPool Pool => ThrowHelper.Required(_pool, "Start the peer first.");

    internal ReplicaAppendStream Transport => ThrowHelper.Required(_transport, "Start the peer first.");

    public override async Task AppendReplicaEntriesStream(
        IAsyncStreamReader<ReplicaAppendStreamRequest> requestStream,
        IServerStreamWriter<ReplicaAppendStreamResponse> responseStream,
        ServerCallContext context)
    {
        _ = Interlocked.Increment(ref _streams);
        if (Failure is { } failure)
            throw new RpcException(new Status(failure, "Scripted peer failure."));

        try
        {
            while (await requestStream.MoveNext(context.CancellationToken))
            {
                var request = requestStream.Current;
                var index = Interlocked.Increment(ref _frames);
                _ = Arrived.TrySetResult();
                if (BlockFirst && index == 1)
                    await Release.Task.WaitAsync(context.CancellationToken);

                if (ResponseKind == 3)
                    return;

                var reply = new ReplicaAppendStreamResponse
                {
                    Sequence = ResponseKind == 1 ? request.Sequence + 1 : request.Sequence,
                    Response = ResponseKind == 2 ? null : Reply(request.Request),
                };
                await responseStream.WriteAsync(reply, context.CancellationToken);
                if (await EndReplyAsync(request, responseStream, context))
                    return;
            }
        }
        finally
        {
            _ = Ended.TrySetResult();
        }
    }

    public override Task<AppendReplicaEntriesResponse> AppendReplicaEntries(AppendReplicaEntriesRequest request, ServerCallContext context)
    {
        _ = Interlocked.Increment(ref _unary);
        return Task.FromResult(Reply(request));
    }

    public async ValueTask DisposeAsync()
    {
        _transport?.Dispose();
        if (_pool != null)
            await _pool.DisposeAsync();

        if (_app != null)
            await _app.DisposeAsync();

        _meter.Dispose();
    }

    internal async Task StartAsync(CancellationToken cancellationToken)
    {
        var builder = WebApplication.CreateBuilder();
        _ = builder.Logging.ClearProviders();
        _ = builder.WebHost.ConfigureKestrel(static options => options.Listen(IPAddress.Loopback, 0, static endpoint => endpoint.Protocols = HttpProtocols.Http2));
        _ = builder.Services.AddGrpc();
        _ = builder.Services.AddSingleton(this);
        _app = builder.Build();
        _ = _app.MapGrpcService<AppendStreamPeer>();
        await _app.StartAsync(cancellationToken);
        var addresses = new List<string>(_app.Urls);
        var peer = new ServerPeer { NodeId = "peer", Uri = new Uri(addresses[0]) };
        _pool = new ServerClientPool(
            [peer],
            new ServerClientPoolArgs
            {
                TimeProvider = TimeProvider.System,
                PolicyFactory = static _ => new IdleCallPolicy(),
            },
            new ServerClientPoolMetrics(_meter),
            NullLogger<ServerClientPool>.Instance,
            NullLogger<PeerHandshakeVerifier>.Instance);
        _transport = new ReplicaAppendStream(Pool);
    }

    internal Task<AppendReplicaEntriesResponse> SendAsync(string group, ulong term, CancellationToken cancellationToken) =>
        Transport.SendAsync("peer", new AppendReplicaEntriesRequest { Header = new ReplicationEnvelopeHeader { GroupId = group, Term = term } }, cancellationToken);

    internal Task StopAsync(CancellationToken cancellationToken) => ThrowHelper.Required(_app, "Start the peer first.").StopAsync(cancellationToken);

    private static AppendReplicaEntriesResponse Reply(AppendReplicaEntriesRequest request) => new() { Success = true, Term = request.Header.Term, LastLogIndex = request.PrevLogIndex };

    private async Task<bool> EndReplyAsync(ReplicaAppendStreamRequest request, IServerStreamWriter<ReplicaAppendStreamResponse> responseStream, ServerCallContext context)
    {
        if (AfterReplyKind == 0)
            return false;

        await ResumeAfterReply.Task.WaitAsync(context.CancellationToken);
        switch (AfterReplyKind)
        {
            case 1:
                return true;
            case 2:
                throw new RpcException(new Status(StatusCode.Unavailable, "Scripted idle failure."));
            case 3:
                await responseStream.WriteAsync(new ReplicaAppendStreamResponse { Sequence = request.Sequence + 1, Response = Reply(request.Request) }, context.CancellationToken);
                return false;
            default:
                throw new InvalidOperationException("Unsupported scripted idle reply.");
        }
    }
}
