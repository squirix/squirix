using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Utils;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Cache;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>
/// Simulates a lost response on the hop from an entry node to a key owner: the owner executes the forwarded mutation and records its outcome,
/// but the entry node never receives the response and every further forward of that key from the entry node fails as unreachable.
/// </summary>
/// <remarks>Only SetEntry, Remove, TryAddEntry, GetOrAdd and distributed access forwards of armed keys are affected.</remarks>
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
    internal void Register(TestServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        TestServiceRegistration? original = null;
        for (var i = services.Count - 1; i >= 0 && original == null; i--)
        {
            if (services[i].ServiceType == typeof(IServerClientPool))
                original = services[i];
        }

        var factory = ThrowHelper.Required(original?.ImplementationFactory, "The node does not register a factory-built client pool.");
        _ = services.Remove(original!);
        _ = services.AddSingleton<IServerClientPool>(sp => new LossyPool(ThrowHelper.Required(factory(sp) as IServerClientPool, "The client pool factory returned another type."), this));
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

    private sealed class LossyClient : IPeerClient
    {
        private readonly IPeerClient _inner;
        private readonly LostForwardResponseProbe _probe;

        internal LossyClient(IPeerClient inner, LostForwardResponseProbe probe)
        {
            _inner = inner;
            _probe = probe;
        }

        public async Task<T> SendAsync<T>(PeerOperation operation, WireMessage request, WireParser<T> parser, CancellationToken cancellationToken)
            where T : WireMessage
        {
            var key = request switch
            {
                RemoveAsyncRequest r => r.Key,
                GetDistributedAsyncRequest r => r.Key,
                GetOrAddAsyncRequest r => r.Key,
                RefreshDistributedAsyncRequest r => r.Key,
                SetEntryAsyncRequest r => r.Key,
                TryAddEntryAsyncRequest r => r.Key,
                _ => null,
            };
            if (key == null || !_probe._faults.TryGetValue(key, out var fault))
                return await _inner.SendAsync(operation, request, parser, cancellationToken).ConfigureAwait(false);

            if (fault.NextAttempt() > 1)
                throw new ServerCallException(FrameStatus.Unavailable, "Injected: key owner connection is down.");

            _ = await _inner.SendAsync(operation, request, parser, cancellationToken).ConfigureAwait(false);
            fault.MarkOwnerExecuted();
            await fault.AfterOwnerExecuted().ConfigureAwait(false);
            throw new ServerCallException(FrameStatus.Unavailable, "Injected: the owner response was lost.");
        }
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

        public IPeerClient ForNode(string nodeId) => new LossyClient(_inner.ForNode(nodeId), _probe);

        public ServerChannelLease LeaseChannel(string nodeId, CancellationToken cancellationToken) => _inner.LeaseChannel(nodeId, cancellationToken);

        public IServerCallPolicy PolicyFor(string nodeId) => _inner.PolicyFor(nodeId);
    }
}
