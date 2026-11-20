using System;
using System.Diagnostics.Metrics;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.Utils;

namespace Squirix.Server.UnitTests.Support;

/// <summary>A loopback mTLS peer that reads the same frames the production pool sends.</summary>
internal sealed class NativePeerFixture : IDisposable
{
    internal const string Node = "node-b";
    internal const string Fingerprint = "test-ring";
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly Meter _meter = new("Squirix");
    private readonly X509Certificate2 _server;
    private readonly PeerHandshakeVerifier _verifier;

    internal NativePeerFixture()
    {
        _server = SharedClusterCertificates.CreateNodeCertificate(Node);
        Material = CreateMaterial();
        _verifier = new PeerHandshakeVerifier(Material.PeerChains!, TimeProvider.System, NullLogger.Instance);
        _listener.Start();
    }

    internal IPEndPoint Endpoint => ThrowHelper.Required(_listener.LocalEndpoint as IPEndPoint, "A loopback endpoint is required.");

    internal MtlsCertificate Material { get; }

    internal SslClientAuthenticationOptions ClientTls => NativePeerClient.CreateTlsOptions("127.0.0.1", Node, Material, _verifier);

    public void Dispose()
    {
        _listener.Dispose();
        IDisposable material = Material;
        material.Dispose();
        _server.Dispose();
        _meter.Dispose();
    }

    internal static Task<Empty> PingAsync(IPeerClient client, CancellationToken cancellationToken) =>
        client.SendAsync("ping", new Empty(), Empty.Parser, cancellationToken);

    internal static async Task<(string Path, string Fingerprint, int Length)> ReadAsync(SslStream stream, CancellationToken cancellationToken)
    {
        var header = GC.AllocateUninitializedArray<byte>(PeerFrames.RequestBytes);
        await stream.ReadExactlyAsync(header, cancellationToken);
        var frame = PeerFrames.ReadRequest(header);
        var payload = new byte[frame.Metadata + frame.Body];
        await stream.ReadExactlyAsync(payload, cancellationToken);
        return (PeerFrames.Text.GetString(payload, 0, frame.Path), PeerFrames.Text.GetString(payload, frame.Path, frame.Fingerprint), frame.Body);
    }

    internal static async Task ReplyAsync(SslStream stream, CancellationToken cancellationToken)
    {
        var header = GC.AllocateUninitializedArray<byte>(PeerFrames.ReplyBytes);
        PeerFrames.WriteReply(header, 0, 0);
        await stream.WriteAsync(header, cancellationToken);
    }

    internal async Task<(string Path, string Fingerprint, int Length)> ExchangePingAsync(IPeerClient client, CancellationToken cancellationToken)
    {
        var call = PingAsync(client, cancellationToken);
        await using var stream = await AcceptTlsAsync(cancellationToken);
        var frame = await ReadAsync(stream, cancellationToken);
        await ReplyAsync(stream, cancellationToken);
        _ = await call;
        return frame;
    }

    internal ServerClientPool CreatePool(TimeSpan? dial = null, Func<CancellationToken, ValueTask<EndPoint>>? resolve = null, SslClientAuthenticationOptions? tls = null, Uri? target = null)
    {
        var address = target ?? new UriBuilder("https", "127.0.0.1", Endpoint.Port).Uri;
        var peer = new ServerPeer { NodeId = Node, Uri = address, InterNodeUri = address };
        var args = new ServerClientPoolArgs
        {
            PolicyFactory = id => ForwardingCallPolicyDefaults.Create(new ServerCallPolicyInstrumentation(new ServerCallPolicyMetrics(_meter), new ServerRpcTimeoutMetrics(_meter)), id, 64, TimeProvider.System),
            TimeProvider = TimeProvider.System,
            ForwardConnectTimeout = dial,
            RingFingerprint = Fingerprint,
            Certificate = Material,
            InterNodeMtlsEnabled = true,
            MtlsOptions = new MtlsOptions { InternalListenPort = Endpoint.Port },
            SocketOptions = resolve == null && tls == null ? null : _ => new PeerSocketOptions(tls ?? NativePeerClient.CreateTlsOptions(address.Host, Node, Material, _verifier), resolve),
        };
        return new ServerClientPool([peer], args, new ServerClientPoolMetrics(_meter), NullLogger<ServerClientPool>.Instance, NullLogger.Instance);
    }

    internal Task<TcpClient> AcceptTcpAsync(CancellationToken cancellationToken) => _listener.AcceptTcpClientAsync(cancellationToken).AsTask();

    internal async Task<SslStream> AcceptTlsAsync(CancellationToken cancellationToken)
    {
        var socket = await _listener.AcceptSocketAsync(cancellationToken);
        var stream = new SslStream(new NetworkStream(socket, true));
        try
        {
            var options = new SslServerAuthenticationOptions
            {
                ServerCertificate = _server,
                ClientCertificateRequired = true,
                ApplicationProtocols = [PeerFrames.Protocol],
                RemoteCertificateValidationCallback = (_, cert, _, _) => cert is X509Certificate2 presented && _verifier.VerifyExpectedPeer(presented, "node-a"),
            };
            await stream.AuthenticateAsServerAsync(options, cancellationToken);
            return stream;
        }
        catch
        {
            await stream.DisposeAsync();
            throw;
        }
    }

    private static MtlsCertificate CreateMaterial()
    {
        X509Certificate2? client = null;
        X509Certificate2? authority = null;
        try
        {
            client = SharedClusterCertificates.CreateNodeCertificate("node-a");
            authority = SharedClusterCertificates.CreateTrustAnchor();
            var material = MtlsCertificate.Create(client, authority);
            client = null;
            authority = null;
            return material;
        }
        finally
        {
            client?.Dispose();
            authority?.Dispose();
        }
    }
}
