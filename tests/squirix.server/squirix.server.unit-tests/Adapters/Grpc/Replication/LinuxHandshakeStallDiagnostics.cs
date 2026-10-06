using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Observability;
using Squirix.Server.UnitTests.Support;
using TUnit.Core;

#pragma warning disable // Temporary diagnostics: output via Console and loosely structured helpers are intentional.

namespace Squirix.Server.UnitTests.Adapters.Grpc.Replication;

/// <summary>Temporary diagnostics that time the first internode mTLS handshake against a loopback stub peer; always passes.</summary>
[NotInParallel]
public sealed class LinuxHandshakeStallDiagnostics : ServerUnitTestBase
{
    private const string LocalNodeId = "node-a";

    private const string PeerNodeId = "node-b";

    private const int Attempts = 30;

    private const double SlowMs = 1000;

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(8);

    private enum Variant
    {
        Pool,
        PlainHandler,
        RawSslStream,
    }

    /// <summary>Runs the three variants 30 times each and prints per-attempt timings and a summary.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task TimeFirstHandshake(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var nodeCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, LocalNodeId);
        using var material = MtlsCertificate.Create(nodeCertificate, bundle.Ca);
        var results = new List<Attempt>();
        for (var i = 0; i < Attempts; i++)
        {
            foreach (var variant in new[] { Variant.Pool, Variant.PlainHandler, Variant.RawSslStream })
            {
                var attempt = await RunAttemptAsync(variant, i, material, nodeCertificate, bundle.Ca, cancellationToken);
                results.Add(attempt);
                Console.WriteLine(attempt.Format());
            }
        }

        PrintSummary("all", results);
    }

    /// <summary>Runs raw SslStream attempts while thread-pool threads spin on CPU-bound loops for three seconds.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task TimeRawHandshakeUnderCpuStarvation(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var nodeCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, LocalNodeId);
        using var material = MtlsCertificate.Create(nodeCertificate, bundle.Ca);
        var window = Stopwatch.StartNew();
        var burners = new List<Task>();
        for (var i = 0; i < 2 * Environment.ProcessorCount; i++)
        {
            burners.Add(Task.Run(() =>
            {
                while (window.Elapsed < TimeSpan.FromSeconds(3))
                {
                }
            }, CancellationToken.None));
        }

        var results = new List<Attempt>();
        var index = 0;
        while (window.Elapsed < TimeSpan.FromSeconds(3) || index < 3)
        {
            var attempt = await RunAttemptAsync(Variant.RawSslStream, index++, material, nodeCertificate, bundle.Ca, cancellationToken);
            results.Add(attempt);
            Console.WriteLine("[starved] " + attempt.Format());
        }

        await Task.WhenAll(burners);
        PrintSummary("starved", results);
    }

    private static async Task<Attempt> RunAttemptAsync(Variant variant, int index, MtlsCertificate material, X509Certificate2 nodeCertificate, X509Certificate2 ca, CancellationToken cancellationToken)
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        var attempt = new Attempt { Variant = variant, Index = index, Clock = Stopwatch.StartNew() };
        attempt.StartThreads = Threads();
        using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var stub = RunStubAsync(listener, attempt, attemptCts.Token);
        var work = RunClientAsync(variant, port, material, nodeCertificate, ca, attemptCts.Token);

        var first = await Task.WhenAny(attempt.FirstByte.Task, work).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        attempt.OutcomeMs = attempt.Clock.Elapsed.TotalMilliseconds;
        if (ReferenceEquals(first, work))
            attempt.Outcome = work.IsCompletedSuccessfully ? "call completed ok" : "call faulted: " + work.Exception?.GetBaseException().GetType().Name + " " + work.Exception?.GetBaseException().Message;
        else
            attempt.Outcome = "first byte seen, call still pending";

        await attemptCts.CancelAsync();
        try
        {
            await work.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        }
        catch (Exception)
        {
            // Teardown noise of a deliberately abandoned attempt.
        }

        attempt.TeardownMs = attempt.Clock.Elapsed.TotalMilliseconds;
        listener.Stop();
        try
        {
            await stub;
        }
        catch (Exception)
        {
            // The stub ends with the cancellation.
        }

        return attempt;
    }

    private static async Task RunClientAsync(Variant variant, int port, MtlsCertificate material, X509Certificate2 nodeCertificate, X509Certificate2 ca, CancellationToken cancellationToken)
    {
        await Task.Yield();
        switch (variant)
        {
            case Variant.Pool:
            {
                using var meter = new Meter("Squirix");
                var peerUri = new Uri($"https://127.0.0.1:{port}");
                var peer = new ServerPeer { NodeId = PeerNodeId, Uri = peerUri, InterNodeUri = peerUri };
                var args = new ServerClientPoolArgs
                {
                    PolicyFactory = static _ => new IdleCallPolicy(),
                    Certificate = material,
                    InterNodeMtlsEnabled = true,
                    MtlsOptions = new MtlsOptions { InternalListenPort = port },
                };
                await using var pool = new ServerClientPool([peer], args, new ServerClientPoolMetrics(meter), NullLogger<ServerClientPool>.Instance);
                var gateway = new ReplicaRpcGateway(pool);
                await gateway.AppendEntriesAsync(PeerNodeId, new ReplicaRpcHeader(LocalNodeId, ReadOnlyMemory<byte>.Empty, 1, 1, LocalNodeId, LocalNodeId), new FollowerBatch([], LocalNodeId, 1, 0, 0, 0), cancellationToken);
                break;
            }

            case Variant.PlainHandler:
            {
                using var productHandler = ServerClientPool.ServerGrpcEndpoints.CreateMtlsHandler(nodeCertificate, ca, PeerNodeId, new TrackedConnections());
                using var handler = new SocketsHttpHandler
                {
                    UseProxy = false,
                    EnableMultipleHttp2Connections = true,
                    ConnectTimeout = TimeSpan.FromSeconds(5),
                    SslOptions = productHandler.SslOptions,
                };
                using var client = new HttpClient(handler);
                using var request = new HttpRequestMessage(HttpMethod.Get, $"https://127.0.0.1:{port}/") { Version = HttpVersion.Version20, VersionPolicy = HttpVersionPolicy.RequestVersionExact };
                using var response = await client.SendAsync(request, cancellationToken);
                break;
            }

            default:
            {
                using var productHandler = ServerClientPool.ServerGrpcEndpoints.CreateMtlsHandler(nodeCertificate, ca, PeerNodeId, new TrackedConnections());
                var source = productHandler.SslOptions;
                var options = new SslClientAuthenticationOptions
                {
                    TargetHost = "127.0.0.1",
                    ClientCertificates = source.ClientCertificates,
                    ClientCertificateContext = source.ClientCertificateContext,
                    ApplicationProtocols = source.ApplicationProtocols,
                    RemoteCertificateValidationCallback = source.RemoteCertificateValidationCallback,
                };
                using var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
                await socket.ConnectAsync(new DnsEndPoint("127.0.0.1", port), cancellationToken);
                using var network = new NetworkStream(socket, false);
                using var ssl = new SslStream(network, false);
                await ssl.AuthenticateAsClientAsync(options, cancellationToken);
                break;
            }
        }
    }

    private static async Task RunStubAsync(TcpListener listener, Attempt attempt, CancellationToken cancellationToken)
    {
        try
        {
            using var client = await listener.AcceptSocketAsync(cancellationToken);
            attempt.AcceptedMs = attempt.Clock.Elapsed.TotalMilliseconds;
            var buffer = new byte[1024];
            var read = await client.ReceiveAsync(buffer, SocketFlags.None, cancellationToken);
            if (read > 0)
            {
                attempt.FirstByteMs = attempt.Clock.Elapsed.TotalMilliseconds;
                attempt.FirstByteThreads = Threads();
                _ = attempt.FirstByte.TrySetResult();
            }

            while (read > 0)
                read = await client.ReceiveAsync(buffer, SocketFlags.None, cancellationToken);
        }
        catch (Exception)
        {
            // Cancelled or reset at teardown.
        }
    }

    private static string Threads() =>
        string.Create(CultureInfo.InvariantCulture, $"threads={ThreadPool.ThreadCount} pending={ThreadPool.PendingWorkItemCount} completed={ThreadPool.CompletedWorkItemCount}");

    private static void PrintSummary(string label, List<Attempt> results)
    {
        Console.WriteLine($"=== SUMMARY {label} ===");
        foreach (var variant in Enum.GetValues<Variant>())
        {
            var times = new List<double>();
            var slow = 0;
            var none = 0;
            foreach (var r in results)
            {
                if (r.Variant != variant)
                    continue;
                if (r.FirstByteMs < 0)
                    none++;
                else
                    times.Add(r.FirstByteMs);
                if (r.FirstByteMs < 0 || r.FirstByteMs > SlowMs || r.OutcomeMs > SlowMs)
                    slow++;
            }

            times.Sort();
            var median = times.Count == 0 ? double.NaN : times[times.Count / 2];
            var max = times.Count == 0 ? double.NaN : times[^1];
            Console.WriteLine(string.Create(CultureInfo.InvariantCulture, $"{variant}: n={times.Count + none} firstByte median={median:F1}ms max={max:F1}ms noFirstByte={none} slow(>1s)={slow}"));
        }
    }

    private sealed class Attempt
    {
        internal readonly TaskCompletionSource FirstByte = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Variant Variant;

        internal int Index;

        internal Stopwatch Clock = null!;

        internal double AcceptedMs = -1;

        internal double FirstByteMs = -1;

        internal double OutcomeMs;

        internal double TeardownMs;

        internal string Outcome = string.Empty;

        internal string StartThreads = string.Empty;

        internal string FirstByteThreads = "n/a";

        internal string Format()
        {
            var slow = FirstByteMs < 0 || FirstByteMs > SlowMs || OutcomeMs > SlowMs;
            return string.Create(
                CultureInfo.InvariantCulture,
                $"{(slow ? "!!SLOW!! " : string.Empty)}{Variant} #{Index}: accepted={AcceptedMs:F1}ms firstByte={FirstByteMs:F1}ms outcome@{OutcomeMs:F1}ms [{Outcome}] teardown@{TeardownMs:F1}ms | start {StartThreads} | firstByte {FirstByteThreads}");
        }
    }
}
