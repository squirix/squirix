using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.IntegrationTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Integration checks for production group-index ordering under concurrent callers.</summary>
public sealed class ConcurrentMutationOrderingTests : NodeIntegrationTestBase
{
    /// <summary>Concurrent mutations retain distinct increasing group indexes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentMutationsUseIncreasingIndexes(CancellationToken cancellationToken)
    {
        const int operationCount = 8;
        var pipeline = new ConformanceTestKit.Pipeline(blockFirstLocalAppend: true);
        var coordinator = ConformanceTestKit.CreateCoordinator(pipeline, operationCount);
        try
        {
            var operations = new Task<ReadOnlyMemory<byte>>[operationCount];
            var firstOperation = coordinator.CommitAsync(ConformanceTestKit.CreateMutation(1), TimeSpan.FromSeconds(2), cancellationToken);
            operations[0] = firstOperation.AsTask();
            await pipeline.FirstLocalAppendStarted.WaitAsync(cancellationToken);
            for (var index = 1; index < operations.Length; index++)
            {
                var logIndex = Convert.ToUInt64(index) + 1;
                var operation = coordinator.CommitAsync(ConformanceTestKit.CreateMutation(logIndex), TimeSpan.FromSeconds(2), cancellationToken);
                operations[index] = operation.AsTask();
            }

            _ = await Assert.That(operations[1..]).All(static operation => !operation.IsCompleted);
            pipeline.ReleaseFirstLocalAppend();
            _ = await Task.WhenAll(operations);

            _ = await Assert.That(pipeline.LocalIndexes.Count).IsEqualTo(operationCount);
            for (var index = 0; index < pipeline.LocalIndexes.Count; index++)
                _ = await Assert.That(pipeline.LocalIndexes[index]).IsEqualTo(Convert.ToUInt64(index) + 1);
        }
        finally
        {
            pipeline.ReleaseFirstLocalAppend();
            await coordinator.DisposeAsync();
        }
    }
}
