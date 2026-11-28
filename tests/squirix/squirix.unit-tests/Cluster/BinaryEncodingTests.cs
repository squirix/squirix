using System;
using System.Net.Security;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Internal.Cluster.Transport;
using Squirix.TestKit;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests.Cluster;

/// <summary>Checks request encoding failures against the TLS stream shared by subsequent calls.</summary>
public sealed class BinaryEncodingTests
{
    /// <summary>A malformed request writes no frame and the next large request and reply keep their identity.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public Task InvalidTextDoesNotBreakTheNextFrame(CancellationToken cancellationToken) =>
        AsyncBinaryCompletionTests.WithPeerAsync(VerifyFramesAsync, cancellationToken);

    private static async Task VerifyFramesAsync(BinaryCacheClient client, SslStream peer, CancellationToken token)
    {
        var options = new BinaryCallOptions(DateTime.UtcNow.AddMinutes(1), token, token);
        var malformed = new GetDistributedAsyncRequest { CacheName = "items", Key = "invalid\ud800" };
        var invalid = client.GetDistributedAsync(malformed, in options);
        var request = new GetDistributedAsyncRequest { CacheName = "items", Key = new string('x', Frames.MaxBatchBytes) };
        var valid = client.GetDistributedAsync(request, in options).AsTask();

        _ = await AsyncAssert.ThrowsAsync<EncoderFallbackException, GetDistributedAsyncResponse>(invalid, TimeSpan.FromSeconds(5), TimeProvider.System, token);
        var id = await AsyncBinaryCompletionTests.ReadRequestAsync(peer, BinaryOperation.GetDistributed, request, token);
        var response = new GetDistributedAsyncResponse { Found = true, Value = new byte[] { 17 } };
        await AsyncBinaryCompletionTests.ReplyAsync(peer, id, response, token);
        var received = await valid.WaitAsync(token);
        _ = await Assert.That(received.Found).IsTrue();
        _ = await Assert.That(received.Value.Span.SequenceEqual(response.Value.Span)).IsTrue();
    }
}
