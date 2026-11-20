using System.Security.Authentication;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>Native peer channels authenticate independent connections and honor configured TLS settings.</summary>
public sealed class PeerChannelsTests
{
    private const string Peer = "node-b";

    /// <summary>The default forward and replication channels establish separate authenticated streams.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ChannelsAuthenticateSeparateSessions(CancellationToken cancellationToken)
    {
        using var peer = new NativePeerFixture();
        await using var pool = peer.CreatePool();
        using var lease = pool.LeaseChannel(Peer, cancellationToken);
        var forward = NativePeerFixture.PingAsync(pool.ForNode(Peer), cancellationToken);
        await using var first = await peer.AcceptTlsAsync(cancellationToken);
        var replication = NativePeerFixture.PingAsync(lease.Client, lease.Token);
        await using var second = await peer.AcceptTlsAsync(cancellationToken);

        _ = await Assert.That(pool.OpenConnections).IsEqualTo(2);
        _ = await Assert.That((await NativePeerFixture.ReadAsync(first, cancellationToken)).Fingerprint).IsEqualTo(NativePeerFixture.Fingerprint);
        _ = await Assert.That((await NativePeerFixture.ReadAsync(second, cancellationToken)).Fingerprint).IsEmpty();
        await NativePeerFixture.ReplyAsync(first, cancellationToken);
        await NativePeerFixture.ReplyAsync(second, cancellationToken);
        _ = await Task.WhenAll(forward, replication);
    }

    /// <summary>Both channels honor the supplied TLS protocol and certificate validator.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SuppliedTlsPolicyIsPreserved(CancellationToken cancellationToken)
    {
        using var peer = new NativePeerFixture();
        var options = peer.ClientTls;

        // Restrict this test peer to one protocol to observe that the pool honors the caller's TLS options.
#pragma warning disable CA5398
        const SslProtocols protocol = SslProtocols.Tls12;
        options.EnabledSslProtocols = protocol;
        var validate = options.RemoteCertificateValidationCallback!;
        var validations = 0;
        options.RemoteCertificateValidationCallback = (sender, certificate, chain, errors) =>
        {
            _ = Interlocked.Increment(ref validations);
            return validate(sender, certificate, chain, errors);
        };
        await using var pool = peer.CreatePool(tls: options);
        using var lease = pool.LeaseChannel(Peer, cancellationToken);

        foreach (var client in new[] { pool.ForNode(Peer), lease.Client })
        {
            var ping = NativePeerFixture.PingAsync(client, cancellationToken);
            await using var stream = await peer.AcceptTlsAsync(cancellationToken);
            _ = await Assert.That(stream.SslProtocol).IsEqualTo(protocol);
            _ = await NativePeerFixture.ReadAsync(stream, cancellationToken);
            await NativePeerFixture.ReplyAsync(stream, cancellationToken);
            _ = await ping;
        }

        _ = await Assert.That(validations).IsEqualTo(2);
#pragma warning restore CA5398
    }
}
