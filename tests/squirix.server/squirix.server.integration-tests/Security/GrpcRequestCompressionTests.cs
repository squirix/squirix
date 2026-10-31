using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client;
using Grpc.Net.Compression;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Serialization.Grpc;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>Integration coverage proving the server refuses compressed gRPC requests.</summary>
public sealed class GrpcRequestCompressionTests : NodeIntegrationTestBase
{
    private const string NodeId = "node-request-compression";
    private const string GzipEncoding = "gzip";
    private const string RequestEncodingHeader = "grpc-internal-encoding-request";
    private const string UnsupportedEncodingDetail = "Unsupported grpc-encoding value 'gzip'";

    /// <summary>Verifies a gzip-compressed cache request fails with Unimplemented and is not applied.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompressedCacheRequestIsRefused(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(NodeId, cancellationToken: cancellationToken);

        using var compressingChannel = GrpcChannel.ForAddress(
            cluster[NodeId].Uri,
            new GrpcChannelOptions
            {
                HttpHandler = LoopbackHttp.CreateHandler(),
                DisposeHttpClient = true,
                CompressionProviders = [new GzipCompressionProvider(CompressionLevel.Fastest)],
            });
        var compressingClient = new SquirixCacheService.SquirixCacheServiceClient(compressingChannel);

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            compressingClient.SetEntryAsync(
                new SetEntryAsyncRequest
                {
                    OperationId = RpcOperationIdentity.New(),
                    CacheName = "default",
                    Key = "compressed-request",
                    Entry = new NodeCacheEntry<object?> { Value = "compressed", Version = 1 }.MapToProto(),
                },
                new Metadata { { RequestEncodingHeader, GzipEncoding } },
                cancellationToken: cancellationToken).ResponseAsync);

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unimplemented);
        _ = await Assert.That(ex.Status.Detail).Contains(UnsupportedEncodingDetail);

        using var plainChannel = CreateGrpcChannel(cluster[NodeId].Uri);
        var plainClient = new SquirixCacheService.SquirixCacheServiceClient(plainChannel);
        var getResponse = await plainClient.GetEntryAsync(new GetEntryAsyncRequest { CacheName = "default", Key = "compressed-request" }, cancellationToken: cancellationToken);
        _ = await Assert.That(getResponse.Found).IsFalse();
    }
}
