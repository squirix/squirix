using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Squirix.Server.Core;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Node.App.Decorators;

/// <summary>Verifies admission shaping runs outside the pipeline deadline (issue #456).</summary>
public sealed class PipelineDeadlineOrderTests : ServerUnitTestBase
{
    /// <summary>Admission observes the caller token, while execution underneath still runs under the pipeline deadline.</summary>
    [Fact]
    public async Task AdmissionBypassesPipelineDeadline()
    {
        var gate = new RecordingGate();
        var inner = new RecordingInnerCache(false);
        var pipeline = CreatePipeline(gate, inner, TimeSpan.FromSeconds(10));

        _ = await pipeline.GetValueAsync("c", "k", CancellationToken.None);

        Assert.False(gate.ObservedToken.CanBeCanceled);
        Assert.True(inner.ObservedToken.CanBeCanceled);
    }

    /// <summary>A hung execution still faults with TimeoutException once the pipeline deadline expires.</summary>
    [Fact]
    public async Task SlowExecutionStillHitsPipelineDeadline()
    {
        var gate = new RecordingGate();
        var inner = new RecordingInnerCache(true);
        var pipeline = CreatePipeline(gate, inner, TimeSpan.FromMilliseconds(100));

        _ = await NodeAsyncAssert.ThrowsAsync<TimeoutException, NodeCacheValueResult<string>>(pipeline.GetValueAsync("c", "k", CancellationToken.None));
    }

    private static BackpressureCacheDecorator<string> CreatePipeline(RecordingGate gate, RecordingInnerCache inner, TimeSpan budget)
    {
        var deadline = new DeadlineCacheDecorator<string>(inner, Options.Create(new CachePipelineDeadlineOptions { DefaultOperationTimeout = budget }));
        return new BackpressureCacheDecorator<string>(deadline, gate, new FixedClientIdResolver("test"));
    }

    private sealed class FixedClientIdResolver : IBackpressureClientIdResolver
    {
        private readonly string _clientId;

        internal FixedClientIdResolver(string clientId)
        {
            _clientId = clientId;
        }

        public string Resolve() => _clientId;
    }

    private sealed class RecordingGate : IBackpressureGate
    {
        internal CancellationToken ObservedToken { get; private set; }

        public ValueTask<(Decision Decision, Lease Lease)> AcquireAsync(string transport, string operation, string clientId, CancellationToken cancellationToken)
        {
            _ = transport;
            _ = operation;
            _ = clientId;
            ObservedToken = cancellationToken;
            return ValueTask.FromResult((Decision.Accepted(), Lease.Empty));
        }
    }

    private sealed class RecordingInnerCache : ILogicalNamespacedCache<string>
    {
        private readonly bool _hang;

        internal RecordingInnerCache(bool hang)
        {
            _hang = hang;
        }

        internal CancellationToken ObservedToken { get; private set; }

        public ValueTask<NodeCacheEntry<string>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult<NodeCacheEntry<string>?>(null);

        public async ValueTask<NodeCacheValueResult<string>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken)
        {
            _ = cacheName;
            _ = key;
            ObservedToken = cancellationToken;
            if (_hang)
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);

            return new NodeCacheValueResult<string>(false, null);
        }

        public ValueTask<CacheRemoveResult<string>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new CacheRemoveResult<string>(false, null));

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => ValueTask.FromResult(false);

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
            ValueTask.FromResult(false);

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<string> entry, CancellationToken cancellationToken) =>
            ValueTask.FromResult(false);

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, string? value, CancellationToken cancellationToken) => ValueTask.FromResult(false);
    }
}
