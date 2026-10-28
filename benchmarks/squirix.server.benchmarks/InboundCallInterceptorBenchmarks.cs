using System;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Invocation;
using Squirix.Server.TestKit.Mtls;

namespace Squirix.Server.Benchmarks;

/// <summary>Cost of the inbound interceptor admitting an internal owner call from a trusted peer certificate on an open connection.</summary>
[MemoryDiagnoser]
[ShortRunJob]
public class InboundCallInterceptorBenchmarks
{
    private const string PeerNodeId = "bench-node-b";
    private const int InternalPort = 6001;
    private static readonly Task<string> CompletedResponse = Task.FromResult("ok");
    private X509Certificate2? _anchor;
    private BenchContext? _context;
    private InboundCallInterceptor? _interceptor;
    private X509Certificate2? _peer;

    /// <summary>Releases the certificates.</summary>
    [GlobalCleanup]
    public void Cleanup()
    {
        _peer?.Dispose();
        _anchor?.Dispose();
    }

    /// <summary>Creates the authority, the peer certificate, the interceptor and a call context that carries the internal owner header.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _anchor = TestCertificates.CreateStandaloneCertificateAuthority();
        _peer = TestCertificates.CreatePeerCertificate(_anchor, PeerNodeId);
        var chains = new PeerChainCache(_anchor, PeerChainCache.DefaultCapacity, PeerChainCache.DefaultMaxAge);
        var peers = new VerifiedPeerCertificates(chains, [PeerNodeId], TimeProvider.System, NullLogger.Instance);
        var fingerprint = RingFingerprint.Create("cluster", ["n1", "n2"], 128);
        var ring = new RingAgreement(fingerprint, NullLogger<RingAgreement>.Instance);
        _interceptor = new InboundCallInterceptor(
            new BenchScopeFactory(),
            new MtlsOptions { InternalListenPort = InternalPort },
            peers,
            ring,
            new ServerCallCorrelation(NullLogger<ServerCallCorrelation>.Instance, "n1"));
        var httpContext = new DefaultHttpContext();
        httpContext.Connection.ClientCertificate = _peer;
        httpContext.Connection.LocalPort = InternalPort;
        httpContext.Features.Set(InternalListenerMarker.Instance);
        var headers = new Metadata
        {
            { RemoteInvocationContract.InternalOwnerRpcHeaderName, RemoteInvocationContract.InternalOwnerRpcHeaderValue },
            { RemoteInvocationContract.RingFingerprintHeaderName, fingerprint.Value },
        };
        _context = new BenchContext(headers, httpContext);
    }

    /// <summary>Admits one internal owner unary call from a trusted peer and runs a completed handler.</summary>
    /// <returns>The response.</returns>
    [Benchmark]
    public Task<string> TrustedPeerUnaryAsync() => _interceptor!.UnaryServerHandler("request", _context!, static (_, _) => CompletedResponse);

    private sealed class BenchScopeFactory : IRemoteInvocationScopeFactory
    {
        public RemoteInvocationScope EnterRemoteInvocation(bool isInternalOwnerInvocation) => RemoteInvocationContext.EnterRemoteInvocation(isInternalOwnerInvocation);
    }

    private sealed class BenchContext : ServerCallContext
    {
        private const string HttpContextKey = "__HttpContext";

        internal BenchContext(Metadata headers, HttpContext httpContext)
        {
            RequestHeadersCore = headers;
            UserState[HttpContextKey] = httpContext;
        }

        protected override AuthContext AuthContextCore => new(null, [with(StringComparer.Ordinal)]);

        protected override CancellationToken CancellationTokenCore => CancellationToken.None;

        protected override DateTime DeadlineCore => DateTime.MaxValue;

        protected override string HostCore => "localhost";

        protected override string MethodCore => "/Bench.Bench/Unary";

        protected override string PeerCore => "ipv4:127.0.0.1:5001";

        protected override Metadata RequestHeadersCore { get; }

        protected override Metadata ResponseTrailersCore => [];

        protected override Status StatusCore { get; set; } = Status.DefaultSuccess;

        protected override WriteOptions? WriteOptionsCore { get; set; }

        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => throw new NotSupportedException();

        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => Task.CompletedTask;
    }
}
