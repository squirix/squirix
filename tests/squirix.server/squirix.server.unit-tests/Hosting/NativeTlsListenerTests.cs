using System.Buffers;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.TestKit.Logging;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Exercises admission and shutdown of native TLS sessions over loopback sockets.</summary>
public sealed class NativeTlsListenerTests
{
    private const byte Reply = 42;

    /// <summary>Stopping admission preserves the encrypted reply from a handler already running.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task StopAcceptingDrainsAdmittedTlsReply(CancellationToken cancellationToken)
    {
        using var ca = MtlsTestCertificateFactory.CreateAuthority();
        using var generated = MtlsTestCertificateFactory.CreatePeerCertificate(ca, "node-a");
        using var certificate = X509CertificateLoader.LoadPkcs12(generated.Export(X509ContentType.Pfx), null, X509KeyStorageFlags.Exportable);
        using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        socket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        socket.Listen(1);
        var address = socket.LocalEndPoint!;
        using var graceful = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var force = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var admitted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var listener = new NativeTlsListener(socket, new SslServerAuthenticationOptions { ServerCertificate = certificate }, ServeAsync, NullLogger.Instance, 1);
        var running = listener.RunAsync(graceful.Token, force.Token);
        try
        {
            using var client = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            await client.ConnectAsync(address, cancellationToken);
            await using var network = new NetworkStream(client, false);
            await using var tls = new SslStream(network, true);
            var auth = new SslClientAuthenticationOptions
            {
                TargetHost = "node-a",
                RemoteCertificateValidationCallback = (_, peer, _, _) => certificate.Equals(peer),
            };
            await tls.AuthenticateAsClientAsync(auth, cancellationToken);
            await admitted.Task.WaitAsync(cancellationToken);
            await graceful.CancelAsync();
            _ = await Assert.That(running.IsCompleted).IsFalse();
            release.SetResult();
            using var reply = MemoryPool<byte>.Shared.Rent(1);
            await tls.ReadExactlyAsync(reply.Memory[..1], cancellationToken);
            _ = await Assert.That(reply.Memory.Span[0]).IsEqualTo(Reply);
            await running.WaitAsync(cancellationToken);
        }
        finally
        {
            await force.CancelAsync();
            await running.WaitAsync(cancellationToken);
        }

        return;

        async Task ServeAsync(SslStream stream, IPEndPoint remote, CancellationToken stopping, CancellationToken aborted)
        {
            admitted.SetResult();
            await release.Task.WaitAsync(aborted);
            if (stopping.IsCancellationRequested && IPAddress.IsLoopback(remote.Address))
                await stream.WriteAsync(new[] { Reply }, aborted);
        }
    }
}
