using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Attributes;
using Squirix.Internal;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Squirix.Testing;
using Squirix.TestKit;
using Squirix.Transport;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Covers the public client contract for ambiguous durable mutation outcomes.</summary>
[Immutable]
public sealed class CommitOutcomeUnknownClientTests
{
    /// <summary>Maps only the stable unavailable detail and preserves the transport failure.</summary>
    [Test]
    public async Task MapsStableCodeToPublicException()
    {
        var transport = new SquirixTransportException(new CallStatus(CacheTransportStatusCode.Unavailable, CommitOutcomeUnknownException.StableDetail));

        _ = await Assert.That(CommitOutcomeUnknownClassifier.Map(transport)).IsTypeOf<CommitOutcomeUnknownException>();
        var error = CommitOutcomeUnknownClassifier.Map(transport)!;

        _ = await Assert.That(error.Message).IsEqualTo(CommitOutcomeUnknownException.StableDetail);
        _ = await Assert.That(error.InnerException).IsSameReferenceAs(transport);
    }

    /// <summary>Declines to map unrelated unavailable failures and returns null.</summary>
    [Test]
    public async Task OtherUnavailableRemainsTransportError()
    {
        var transport = new SquirixTransportException(new CallStatus(CacheTransportStatusCode.Unavailable, "peer unavailable"));

        var error = CommitOutcomeUnknownClassifier.Map(transport);

        _ = await Assert.That(error).IsNull();
    }

    /// <summary>Surfaces an ambiguous outcome immediately without consuming the retry budget, keeping the operation id.</summary>
    /// <param name="suspend">Whether the transport response suspends before reporting the failure.</param>
    /// <param name="expireBudget">Whether the operation deadline elapses before the response arrives.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false, false)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task UnknownOutcomeStopsWithoutRetryingAsync(bool suspend, bool expireBudget, CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        await using var policy = new CallPolicy(clock, TimeSpan.FromHours(2), 3, TimeSpan.Zero, TimeSpan.Zero, peer: "commit-unknown");
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var transport = new UnknownOutcomeTransport(suspend ? release.Task : Task.CompletedTask);
        await using var pool = SingleNodeClientPool.Create(transport.InvokeAsync, policy);
        var cache = new RemoteCache<string>("demo", new EndpointFailover(["node-0"], "node-0", TimeSpan.FromHours(1), clock), pool, RemoteClientSessionFactory.CreateSerializer());

        var operation = SetAndProjectAsync(cache).AsTask();
        var pending = !operation.IsCompleted;
        if (expireBudget)
            clock.Advance(TimeSpan.FromHours(1));
        _ = release.TrySetResult();
        var error = await AsyncAssert.ThrowsAsync<CommitOutcomeUnknownException, bool>(new ValueTask<bool>(operation), TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);

        _ = await Assert.That(pending).IsEqualTo(suspend);
        _ = await Assert.That(error.Message).IsEqualTo(CommitOutcomeUnknownException.StableDetail);
        _ = await Assert.That(error.InnerException).IsSameReferenceAs(transport.Failures[^1]);
        var operationId = await Assert.That(transport.OperationIds).HasSingleItem();
        _ = await Assert.That(string.IsNullOrEmpty(operationId)).IsFalse();
    }

    private static async ValueTask<bool> SetAndProjectAsync(RemoteCache<string> cache)
    {
        await cache.SetAsync("key-a", "value", cancellationToken: CancellationToken.None).ConfigureAwait(false);
        return true;
    }

    private sealed class UnknownOutcomeTransport
    {
        private readonly Task _release;

        internal UnknownOutcomeTransport(Task release)
        {
            _release = release;
        }

        internal List<SquirixTransportException> Failures { get; } = [];

        internal List<string> OperationIds { get; } = [];

        internal Func<BinaryOperation, IMessage, MessageParser, BinaryCallOptions, Task<IMessage>> InvokeAsync =>
            (_, request, _, _) => HandleAsync(request);

        private static string ExtractOperationId<TRequest>(TRequest request) => request switch
        {
            SetEntryAsyncRequest set => set.OperationId,
            TryAddEntryAsyncRequest add => add.OperationId,
            UpdateAsyncRequest update => update.OperationId,
            GetOrAddAsyncRequest getOrAdd => getOrAdd.OperationId,
            RemoveAsyncRequest remove => remove.OperationId,
            RemoveExpirationAsyncRequest removeExpiration => removeExpiration.OperationId,
            TouchAsyncRequest touch => touch.OperationId,
            _ => string.Empty,
        };

        private static async Task<IMessage> FailAfterReleaseAsync(Task release, SquirixTransportException failure)
        {
            await release.ConfigureAwait(false);
            throw failure;
        }

        private Task<IMessage> HandleAsync(IMessage request)
        {
            var failure = new SquirixTransportException(new CallStatus(CacheTransportStatusCode.Unavailable, CommitOutcomeUnknownException.StableDetail));
            Failures.Add(failure);
            OperationIds.Add(ExtractOperationId(request));
            return FailAfterReleaseAsync(_release, failure);
        }
    }
}
