using System;
using System.Threading.Tasks;
using Squirix.Server.IntegrationTests.Support;
using Xunit;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Integration checks for production group-index ordering under concurrent callers.</summary>
public sealed class ConcurrentMutationOrderingTests : NodeIntegrationTestBase
{
    /// <summary>Concurrent mutations retain distinct increasing group indexes.</summary>
    [Fact]
    public async Task ConcurrentMutationsUseIncreasingIndexes()
    {
        const int operationCount = 8;
        var pipeline = new ConformanceTestKit.Pipeline(blockFirstLocalAppend: true);
        var coordinator = ConformanceTestKit.CreateCoordinator(pipeline, operationCount);
        try
        {
            var operations = new Task<ReadOnlyMemory<byte>>[operationCount];
            var firstOperation = coordinator.CommitAsync(ConformanceTestKit.CreateMutation(1), TimeSpan.FromSeconds(2), DefaultCancellationToken);
            operations[0] = firstOperation.AsTask();
            await pipeline.FirstLocalAppendStarted.WaitAsync(DefaultCancellationToken);
            for (var index = 1; index < operations.Length; index++)
            {
                var logIndex = Convert.ToUInt64(index) + 1;
                var operation = coordinator.CommitAsync(ConformanceTestKit.CreateMutation(logIndex), TimeSpan.FromSeconds(2), DefaultCancellationToken);
                operations[index] = operation.AsTask();
            }

            Assert.All(operations[1..], static operation => Assert.False(operation.IsCompleted));
            pipeline.ReleaseFirstLocalAppend();
            _ = await Task.WhenAll(operations);

            Assert.Equal(operationCount, pipeline.LocalIndexes.Count);
            for (var index = 0; index < pipeline.LocalIndexes.Count; index++)
                Assert.Equal(Convert.ToUInt64(index) + 1, pipeline.LocalIndexes[index]);
        }
        finally
        {
            pipeline.ReleaseFirstLocalAppend();
            await coordinator.DisposeAsync();
        }
    }
}
