using System;
using System.Globalization;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Squirix.Server.Limits;
using Squirix.Server.Node.Cluster.Membership;
using Squirix.Server.TestKit.Limits;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.IntegrationTests.Limits;

/// <summary>
/// Integration coverage for fixed entry payload size limits (issue #2).
/// </summary>
public sealed class EntryPayloadLimitIntegrationTests : IntegrationTestBase
{
    private const string CacheRoute = "/api/v1/cache";
    private const string StorageDiagnosticsRoute = "/admin/diagnostics/storage";

    /// <summary>
    /// Verifies REST insert succeeds when the serialized entry is at or below the fixed limit.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task RestInsertJustBelowLimitSucceeds()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };
        await using var node = await StartNodeAsync(url, peers);

        var value = EntryPayloadLimitTestHelpers.CreateStringValueAtMostRestEntryBytes(SquirixEntryLimits.MaxEntrySizeBytes);
        var response = await HttpClient.PutAsJsonAsync($"{url}{CacheRoute}/within-limit", new CacheEntry<string> { Value = value }, DefaultCancellationToken);

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
    }

    /// <summary>
    /// Verifies REST insert above the limit returns 413, does not persist, and does not append to the journal.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task RestInsertAboveLimitReturns413AndDoesNotPersist()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };
        await using var node = await StartNodeAsync(url, peers);

        var opsBefore = await ReadJournalAppendedOpsAsync(url);
        var value = EntryPayloadLimitTestHelpers.CreateStringValueExceedingEntryLimit();
        var response = await HttpClient.PutAsJsonAsync($"{url}{CacheRoute}/over-limit", new CacheEntry<string> { Value = value }, DefaultCancellationToken);

        Assert.Equal((HttpStatusCode)StatusCodes.Status413PayloadTooLarge, response.StatusCode);
        var payload = await response.Content.ReadFromJsonAsync<JsonElement>(DefaultCancellationToken);
        Assert.Equal("PayloadTooLarge", payload.GetProperty("error").GetString());

        var getResponse = await HttpClient.GetAsync($"{url}{CacheRoute}/over-limit", DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.NotFound, getResponse.StatusCode);

        var opsAfter = await ReadJournalAppendedOpsAsync(url);
        Assert.Equal(opsBefore, opsAfter);
    }

    /// <summary>
    /// Verifies gRPC insert above the limit returns ResourceExhausted and does not persist.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task GrpcInsertAboveLimitReturnsResourceExhaustedAndDoesNotPersist()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };
        await using var node = await StartNodeAsync(url, peers);

        using var channel = CreateGrpcChannel(url);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var value = EntryPayloadLimitTestHelpers.CreateStringValueExceedingEntryLimit();

        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await client.InsertAsync(
                new InsertRequest
                {
                    CacheName = "default",
                    Key = "grpc-over-limit",
                    Entry = new CacheEntry<object?> { Value = value, Version = 1 }.MapToProto(),
                },
                cancellationToken: DefaultCancellationToken);
        });

        Assert.Equal(StatusCode.ResourceExhausted, ex.StatusCode);
        Assert.Contains(SquirixEntryLimits.MaxEntrySizeBytes.ToString(CultureInfo.InvariantCulture), ex.Status.Detail, StringComparison.Ordinal);

        var getResponse = await HttpClient.GetAsync($"{url}{CacheRoute}/grpc-over-limit", DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.NotFound, getResponse.StatusCode);
    }

    /// <summary>
    /// Verifies cluster forwarding preserves PayloadTooLarge when the remote owner rejects an oversized entry.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task ClusterForwardPreservesPayloadTooLargeForRemoteOwner()
    {
        var urlA = GetNextHttpUrl();
        var urlB = GetNextHttpUrl();
        var peers = new[]
        {
            new Peer { NodeId = "node-a", Url = urlA },
            new Peer { NodeId = "node-b", Url = urlB },
        };

        await using var nodeA = await StartNodeAsync(urlA, peers);
        await using var nodeB = await StartNodeAsync(urlB, peers);

        var key = await FindKeyOwnedByAsync(urlA, "node-b");
        var value = EntryPayloadLimitTestHelpers.CreateStringValueExceedingEntryLimit();
        var response = await HttpClient.PutAsJsonAsync($"{urlA}{CacheRoute}/{key}", new CacheEntry<string> { Value = value }, DefaultCancellationToken);

        Assert.Equal((HttpStatusCode)StatusCodes.Status413PayloadTooLarge, response.StatusCode);

        var getOnOwner = await HttpClient.GetAsync($"{urlB}{CacheRoute}/{key}", DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.NotFound, getOnOwner.StatusCode);
    }

    private async Task<string> FindKeyOwnedByAsync(string nodeAddress, string expectedOwner)
    {
        for (var i = 0; i < 2000; i++)
        {
            var key = $"k{i:0000}";
            var who = await HttpClient.GetStringAsync($"{nodeAddress}/admin/owner/{Uri.EscapeDataString(key)}", DefaultCancellationToken);
            if (who.Contains($"\"owner\":\"{expectedOwner}\"", StringComparison.Ordinal))
                return key;
        }

        throw new InvalidOperationException($"Failed to find a key owned by {expectedOwner}");
    }

    private async Task<long> ReadJournalAppendedOpsAsync(string url)
    {
        var diagnostics = await HttpClient.GetFromJsonAsync<JsonElement>($"{url}{StorageDiagnosticsRoute}", DefaultCancellationToken);
        return diagnostics.GetProperty("writer").GetProperty("appendedOps").GetInt64();
    }
}
