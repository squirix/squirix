using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>
/// Simulates a lost response on the hop from an entry node to a key owner: the owner executes the forwarded mutation and records its outcome,
/// but the entry node never receives the response and every further forward of that key from the entry node fails as unreachable.
/// </summary>
/// <remarks>Register the probe on the entry node only. Only <c language="csharp">SetEntry</c>, <c language="csharp">TryAddEntry</c> and <c language="csharp">GetOrAdd</c> forwards of armed keys are affected.</remarks>
internal sealed class LostForwardResponseProbe
{
    private readonly ConcurrentDictionary<string, KeyFault> _faults = new(StringComparer.Ordinal);

    /// <summary>Arms the fault for one key.</summary>
    /// <param name="key">The key whose first forwarded mutation loses its response.</param>
    /// <param name="afterOwnerExecuted">Runs once after the owner executed the first forward and before the entry node observes the loss.</param>
    internal void Lose(string key, Func<Task> afterOwnerExecuted) => _faults[key] = new KeyFault(afterOwnerExecuted);

    /// <summary>Gets the number of forwards the entry node attempted for the key, including the ones refused without reaching the owner.</summary>
    /// <param name="key">The armed key.</param>
    /// <returns>The attempt count.</returns>
    internal int ForwardAttempts(string key) => _faults[key].Attempts;

    /// <summary>Gets the number of forwards for the key that reached the owner and completed there.</summary>
    /// <param name="key">The armed key.</param>
    /// <returns>The count of owner executions.</returns>
    internal int OwnerExecutions(string key) => _faults[key].OwnerExecutions;

    /// <summary>Wraps the node client pool so forwarded calls pass through the fault.</summary>
    /// <param name="services">The node service collection.</param>
    /// <exception cref="InvalidOperationException">Thrown when the node does not register a factory-built client pool.</exception>
    internal void Register(IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        ServiceDescriptor? original = null;
        for (var i = services.Count - 1; i >= 0 && original == null; i--)
        {
            if (services[i].ServiceType == typeof(IServerClientPool))
                original = services[i];
        }

        var factory = ThrowHelper.Required(original?.ImplementationFactory, "The node does not register a factory-built client pool.");
        _ = services.Remove(original!);
        _ = services.AddSingleton<IServerClientPool>(sp => new LossyPool(ThrowHelper.Required(factory(sp) as IServerClientPool, "The client pool factory returned another type."), this));
    }

    private static AsyncUnaryCall<TResponse> Unreachable<TResponse>()
    {
        var status = new Status(StatusCode.Unavailable, "Injected: key owner connection is down.");
        return new AsyncUnaryCall<TResponse>(
            Task.FromException<TResponse>(new RpcException(status)),
            Task.FromResult(new Metadata()),
            () => status,
            static () => [],
            static () => { });
    }

    private static async Task<TResponse> LoseResponseAsync<TResponse>(KeyFault fault, AsyncUnaryCall<TResponse> call)
    {
#pragma warning disable VSTHRD003 // The call was started by the forwarder through this probe; its response is the signal that the owner executed.
        _ = await call.ResponseAsync.ConfigureAwait(false);
#pragma warning restore VSTHRD003
        fault.MarkOwnerExecuted();
        await fault.AfterOwnerExecuted().ConfigureAwait(false);
        throw new RpcException(new Status(StatusCode.Unavailable, "Injected: the owner response was lost."));
    }

    private AsyncUnaryCall<TResponse> Forward<TResponse>(string key, Func<AsyncUnaryCall<TResponse>> send)
    {
        if (!_faults.TryGetValue(key, out var fault))
            return send();

        if (fault.NextAttempt() > 1)
            return Unreachable<TResponse>();

        var call = send();
        return new AsyncUnaryCall<TResponse>(LoseResponseAsync(fault, call), call.ResponseHeadersAsync, call.GetStatus, call.GetTrailers, call.Dispose);
    }

    private sealed class KeyFault
    {
        private int _attempts;
        private int _ownerExecutions;

        internal KeyFault(Func<Task> afterOwnerExecuted)
        {
            AfterOwnerExecuted = afterOwnerExecuted;
        }

        internal Func<Task> AfterOwnerExecuted { get; }

        internal int Attempts => Volatile.Read(ref _attempts);

        internal int OwnerExecutions => Volatile.Read(ref _ownerExecutions);

        internal void MarkOwnerExecuted() => _ = Interlocked.Increment(ref _ownerExecutions);

        internal int NextAttempt() => Interlocked.Increment(ref _attempts);
    }

    private sealed class LossyClient : SquirixCacheService.SquirixCacheServiceClient
    {
        private readonly SquirixCacheService.SquirixCacheServiceClient _inner;
        private readonly LostForwardResponseProbe _probe;

        internal LossyClient(SquirixCacheService.SquirixCacheServiceClient inner, LostForwardResponseProbe probe)
        {
            _inner = inner;
            _probe = probe;
        }

        public override AsyncUnaryCall<GetOrAddAsyncResponse> GetOrAddAsync(GetOrAddAsyncRequest request, CallOptions options) =>
            _probe.Forward(request.Key, () => _inner.GetOrAddAsync(request, options));

        public override AsyncUnaryCall<SetAsyncResponse> SetEntryAsync(SetEntryAsyncRequest request, CallOptions options) =>
            _probe.Forward(request.Key, () => _inner.SetEntryAsync(request, options));

        public override AsyncUnaryCall<TryAddAsyncResponse> TryAddEntryAsync(TryAddEntryAsyncRequest request, CallOptions options) =>
            _probe.Forward(request.Key, () => _inner.TryAddEntryAsync(request, options));
    }

    private sealed class LossyPool : IServerClientPool
    {
        private readonly IServerClientPool _inner;
        private readonly LostForwardResponseProbe _probe;

        internal LossyPool(IServerClientPool inner, LostForwardResponseProbe probe)
        {
            _inner = inner;
            _probe = probe;
        }

        public ValueTask DisposeAsync() => _inner.DisposeAsync();

        public SquirixCacheService.SquirixCacheServiceClient ForNode(string nodeId) => new LossyClient(_inner.ForNode(nodeId), _probe);

        public GrpcChannel OpenChannel(string nodeId) => _inner.OpenChannel(nodeId);

        public IServerCallPolicy PolicyFor(string nodeId) => _inner.PolicyFor(nodeId);
    }
}
