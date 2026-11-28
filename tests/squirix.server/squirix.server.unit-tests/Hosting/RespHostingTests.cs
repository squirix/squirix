using System;
using System.Buffers.Text;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Adapters.Endpoint.Resp;
using Squirix.Server.Composition;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Testing;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies RESP TLS hosting and session negotiation against real sockets.</summary>
public sealed class RespHostingTests
{
    /// <summary>An HTTPS primary endpoint also enables TLS on the RESP listener.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task TlsListenerAcceptsPing(CancellationToken cancellationToken)
    {
        await using var host = await RespTestHost.StartAsync(cancellationToken, new RespOptions { Port = 0 });
        _ = await Assert.That(host.RespUri!.Host).IsEqualTo("127.0.0.1");
        await using var stream = await host.ConnectAsync(cancellationToken);
        await RespCacheTests.ExchangeAsync(stream, RespTestHost.Command("PING"), "+PONG\r\n", cancellationToken);
    }

    /// <summary>Default server options select plain TCP for the binary client endpoint.</summary>
    [Test]
    public async Task DefaultServerUsesTcp()
    {
        var options = new SquirixServerOptions();
        _ = await Assert.That(options.Uri).IsEqualTo(new Uri("tcp://localhost:5001"));
        _ = await Assert.That(options.TryValidate(out _)).IsTrue();
    }

    /// <summary>Disabled RESP never binds its configured port.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task DisabledListenerDoesNotBind(CancellationToken cancellationToken)
    {
        using var occupied = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        occupied.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        occupied.Listen();
        var port = (occupied.LocalEndPoint as IPEndPoint)!.Port;
        await using var host = await RespTestHost.StartAsync(cancellationToken, new RespOptions { Enabled = false, Port = port });

        _ = await Assert.That(host.RespUri).IsNull();
    }

    /// <summary>A bind conflict on RESP releases the native socket reserved by the same startup.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task RespBindConflictRollsBackNativeListener(CancellationToken cancellationToken)
    {
        using var occupied = new TcpListener(IPAddress.Loopback, 0);
        occupied.Start();
        var port = (occupied.LocalEndpoint as IPEndPoint)!.Port;
        Uri? nativeUri = null;

        var startup = RespTestHost.StartAsync(cancellationToken, new RespOptions { Enabled = true, Port = port }, configure: options => nativeUri = options.Uri);
        var error = await NodeAsyncAssert.ThrowsAsync<IOException, RespTestHost>(startup, TimeSpan.FromSeconds(20), TimeProvider.System, cancellationToken);
        _ = await Assert.That(error.Message.Contains("rediss://", StringComparison.Ordinal)).IsTrue();

        using var rebound = new TcpListener(IPAddress.Loopback, nativeUri!.Port);
        rebound.Start();
    }

    /// <summary>Refuses startup when a non-loopback RESP listener would be unauthenticated.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task RejectsUnauthenticatedExternalBinding(CancellationToken cancellationToken)
    {
        var options = new RespOptions { Enabled = true, BindAddress = "0.0.0.0" };
        var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, RespTestHost>(
            RespTestHost.StartAsync(cancellationToken, options), TimeSpan.FromSeconds(20), TimeProvider.System, cancellationToken);

        _ = await Assert.That(error.Message.Contains("Configure JWT settings", StringComparison.Ordinal)).IsTrue();
    }

    /// <summary>Accepts an enabled RESP listener on a valid multi-node topology.</summary>
    /// <param name="replicas">The number of copies of each key.</param>
    [Test]
    [Arguments(1)]
    [Arguments(3)]
    public async Task AcceptsRespOnMultiNodeTopology(int replicas)
    {
        var options = new SquirixServerOptions
        {
            Resp = new RespOptions { Enabled = true },
            ReplicaCount = replicas,
            ReplicationEnabled = replicas > 1,
            PersistenceEnabled = true,
        };
        options.Peers =
        [
            new SquirixServerPeerOptions { NodeId = options.NodeId, Uri = options.Uri },
            new SquirixServerPeerOptions { NodeId = "node-b", Uri = new Uri("https://localhost:6098") },
            new SquirixServerPeerOptions { NodeId = "node-c", Uri = new Uri("https://localhost:6099") },
        ];

        _ = await Assert.That(options.TryValidate(out var errors)).IsTrue();
        _ = await Assert.That(errors.Count).IsEqualTo(0);
    }

    /// <summary>Two negotiated modes, invalid commands and a coalesced pipeline preserve wire order.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task NegotiatesModesAndKeepsPipelineInOrder(CancellationToken cancellationToken)
    {
        await using var host = await RespTestHost.StartAsync(cancellationToken);
        await using var stream = await host.ConnectAsync(cancellationToken);
        var requests = RespTestHost.Command("CLIENT", "GETNAME") + RespTestHost.Command("HELLO", "3") + RespTestHost.Command("CLIENT", "GETNAME") +
                       RespTestHost.Command("HELLO", "4") + RespTestHost.Command("CLIENT", "GETNAME") + RespTestHost.Command("HELLO", "2") +
                       RespTestHost.Command("SELECT", "1") + RespTestHost.Command("SELECT", "0") + RespTestHost.Command("ECHO") + RespTestHost.Command("ping", "end");
        var replies = "$-1\r\n" + RespTestHost.FirstHelloReply(3) +
                      "_\r\n-NOPROTO unsupported protocol version\r\n_\r\n" + RespTestHost.FirstHelloReply(2) +
                      "-ERR only database 0 is supported\r\n+OK\r\n-ERR wrong number of arguments\r\n$3\r\nend\r\n";

        await stream.WriteAsync(Encoding.UTF8.GetBytes(requests), cancellationToken);
        await AssertReplyAsync(stream, replies, cancellationToken);
    }

    /// <summary>Failed authentication leaves the active protocol and prior identity intact.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task ValidatesJwtDuringAuthAndHello(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var token = TestJwtHelper.CreateBearerToken(credentials);
        await using var host = await RespTestHost.StartAsync(cancellationToken, jwt: credentials);
        await using var stream = await host.ConnectAsync(cancellationToken);
        var requests = RespTestHost.Command("PING") + RespTestHost.Command("HELLO", "3") +
                       RespTestHost.Command("AUTH", "default", ExpiredToken(credentials)) + RespTestHost.Command("AUTH", "not-a-token") +
                       RespTestHost.Command("HELLO", "3", "AUTH", "default", token, "SETNAME", "probe") + RespTestHost.Command("CLIENT", "GETNAME") +
                       RespTestHost.Command("HELLO", "2", "AUTH", "default", "wrong") + RespTestHost.Command("HELLO") + RespTestHost.Command("AUTH", token);
        var replies = "-NOAUTH Authentication required.\r\n-NOAUTH Authentication required.\r\n-WRONGPASS invalid username-password pair\r\n" +
                      "-WRONGPASS invalid username-password pair\r\n" + RespTestHost.FirstHelloReply(3) + "$5\r\nprobe\r\n" +
                      "-WRONGPASS invalid username-password pair\r\n" + RespTestHost.FirstHelloReply(3) + "+OK\r\n";

        await stream.WriteAsync(Encoding.UTF8.GetBytes(requests), cancellationToken);
        await AssertReplyAsync(stream, replies, cancellationToken);
    }

    /// <summary>Rejects a write on a connection whose accepted token has expired.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task ExpiredJwtCannotMutateCache(CancellationToken cancellationToken)
    {
        var clock = new WallSteppedClock(TimeProvider.System.GetUtcNow());
        var credentials = TestJwtHelper.CreateRandomCredentials();
        await using var host = await RespTestHost.StartAsync(
            cancellationToken,
            jwt: credentials,
            register: services => _ = services.AddSingleton(provider =>
                new NativeConnectionAuthenticationValidator(provider.Require<Func<JwtValidationOptions>>(), clock, provider)));
        await using var original = await host.ConnectAsync(cancellationToken);
        var now = clock.GetUtcNow();
        var token = CreateToken(credentials, now.AddMinutes(-1), now.AddMinutes(1));
        await original.WriteAsync(Encoding.UTF8.GetBytes(RespTestHost.Command("AUTH", token)), cancellationToken);
        await AssertReplyAsync(original, "+OK\r\n", cancellationToken);

        clock.StepWallClock(TimeSpan.FromMinutes(10));
        var write = RespTestHost.Command("SET", "expired-session", "unexpected");
        await original.WriteAsync(Encoding.UTF8.GetBytes(write), cancellationToken);
        await AssertReplyAsync(original, "-ERR cache operation unavailable\r\n", cancellationToken);
        _ = await Assert.That(await original.ReadAsync(new byte[1], cancellationToken)).IsEqualTo(0);

        await using var renewed = await host.ConnectAsync(cancellationToken);
        now = clock.GetUtcNow();
        var current = CreateToken(credentials, now.AddMinutes(-1), now.AddMinutes(1));
        var read = RespTestHost.Command("AUTH", current) + RespTestHost.Command("GET", "expired-session");
        await renewed.WriteAsync(Encoding.UTF8.GetBytes(read), cancellationToken);
        await AssertReplyAsync(renewed, "+OK\r\n$-1\r\n", cancellationToken);
    }

    /// <summary>Idle connections close when their current, possibly renewed JWT expires.</summary>
    /// <param name="renew">Whether to replace the token before advancing past its first expiry.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    [Timeout(20_000)]
    public async Task IdleSessionClosesAtJwtExpiry(bool renew, CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(TimeProvider.System.GetUtcNow());
        var credentials = TestJwtHelper.CreateRandomCredentials();
        await using var host = await RespTestHost.StartAsync(
            cancellationToken,
            jwt: credentials,
            register: services => _ = services.AddSingleton(provider =>
                new NativeConnectionAuthenticationValidator(provider.Require<Func<JwtValidationOptions>>(), clock, provider)));
        await using var stream = await host.ConnectAsync(cancellationToken);
        var now = clock.GetUtcNow();
        var original = CreateToken(credentials, now.AddMinutes(-1), now.AddMinutes(1));
        await RespCacheTests.ExchangeAsync(stream, RespTestHost.Command("AUTH", original), "+OK\r\n", cancellationToken);
        if (renew)
        {
            var replacement = CreateToken(credentials, now.AddMinutes(-1), now.AddMinutes(10));
            await RespCacheTests.ExchangeAsync(stream, RespTestHost.Command("AUTH", replacement), "+OK\r\n", cancellationToken);
        }

        clock.Advance(TimeSpan.FromMinutes(4));
        if (renew)
        {
            await RespCacheTests.ExchangeAsync(stream, RespTestHost.Command("PING"), "+PONG\r\n", cancellationToken);
            clock.Advance(TimeSpan.FromMinutes(9));
        }

        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(5));
        _ = await Assert.That(await stream.ReadAsync(new byte[1], deadline.Token)).IsEqualTo(0);
    }

    /// <summary>Commands before AUTH do not restart the first authentication deadline.</summary>
    /// <param name="authenticate">Whether to authenticate before the initial deadline.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    [Timeout(20_000)]
    public async Task InitialAuthDeadlineIsNotExtended(bool authenticate, CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(TimeProvider.System.GetUtcNow());
        var credentials = TestJwtHelper.CreateRandomCredentials();
        await using var host = await RespTestHost.StartAsync(
            cancellationToken,
            jwt: credentials,
            register: services => _ = services.AddSingleton<TimeProvider>(clock));
        await using var stream = await host.ConnectAsync(cancellationToken);
        var ping = RespTestHost.Command("PING");
        await RespCacheTests.ExchangeAsync(stream, ping, "-NOAUTH Authentication required.\r\n", cancellationToken);

        clock.Advance(TimeSpan.FromSeconds(4));
        await RespCacheTests.ExchangeAsync(stream, ping, "-NOAUTH Authentication required.\r\n", cancellationToken);
        if (authenticate)
        {
            var command = RespTestHost.Command("AUTH", TestJwtHelper.CreateBearerToken(credentials));
            await RespCacheTests.ExchangeAsync(stream, command, "+OK\r\n", cancellationToken);
        }

        clock.Advance(TimeSpan.FromSeconds(1));
        if (authenticate)
        {
            await RespCacheTests.ExchangeAsync(stream, ping, "+PONG\r\n", cancellationToken);
            return;
        }

        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(5));
        _ = await Assert.That(await stream.ReadAsync(new byte[1], deadline.Token)).IsEqualTo(0);
    }

    /// <summary>If the old session expires during renewal, the server refuses to acknowledge the new identity.</summary>
    /// <param name="hello">Whether the renewal also negotiates RESP3.</param>
    /// <param name="runExpiryTimer">Whether elapsed time also makes the expiry timer fire.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    [Timeout(20_000)]
    public async Task ExpiryDuringRenewalRefusesAuth(bool hello, bool runExpiryTimer, CancellationToken cancellationToken)
    {
        var now = TimeProvider.System.GetUtcNow();
        var clock = new WallSteppedClock(now);
        var credentials = TestJwtHelper.CreateRandomCredentials();
        await using var host = await RespTestHost.StartAsync(
            cancellationToken,
            jwt: credentials,
            register: services =>
            {
                _ = services.AddSingleton<IConnectionTokenObserver>(new ExpireDuringRenewal(clock, runExpiryTimer));
                _ = services.AddSingleton(provider =>
                    new NativeConnectionAuthenticationValidator(provider.Require<Func<JwtValidationOptions>>(), clock, provider));
            });
        await using var stream = await host.ConnectAsync(cancellationToken);
        var first = CreateToken(credentials, now.AddMinutes(-1), now.AddMinutes(1));
        await RespCacheTests.ExchangeAsync(stream, RespTestHost.Command("AUTH", first), "+OK\r\n", cancellationToken);

        var replacement = CreateToken(credentials, now.AddMinutes(-1), now.AddMinutes(10));
        var renewal = hello ? RespTestHost.Command("HELLO", "3", "AUTH", "default", replacement) : RespTestHost.Command("AUTH", replacement);
        await stream.WriteAsync(Encoding.ASCII.GetBytes(renewal), cancellationToken);
        using var reader = new StreamReader(stream, Encoding.ASCII, false, 128, true);
        _ = await Assert.That(await reader.ReadLineAsync(cancellationToken)).IsEqualTo("-NOAUTH Authentication required.");
        _ = await Assert.That(await reader.ReadLineAsync(cancellationToken)).IsNull();
    }

    /// <summary>Malformed lengths close the connection even when a valid command follows.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task FramingErrorClosesBeforeNextCommand(CancellationToken cancellationToken)
    {
        await using var host = await RespTestHost.StartAsync(cancellationToken);
        await using var stream = await host.ConnectAsync(cancellationToken);
        byte[] requests = [.. "*2\r\n$-1\r\n*1\r\n$4\r\nPING\r\n"u8];

        await stream.WriteAsync(requests, cancellationToken);
        await AssertReplyAsync(stream, "-ERR invalid RESP request framing\r\n", cancellationToken);
        _ = await Assert.That(await stream.ReadAsync(new byte[1], cancellationToken)).IsEqualTo(0);
    }

    /// <summary>A blocked reply ends the session without writing replies to later requests.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(20_000)]
    public async Task StalledReaderEndsRespSession(CancellationToken cancellationToken)
    {
        await using var host = await RespTestHost.StartAsync(cancellationToken);
        var services = host.Server.Services;
        var cache = new CacheCommands(services.Require<BinaryDispatch>(), null, TimeProvider.System, false);
        var session = new Session(null, cache, 1, TimeProvider.System);
        var requests = RespTestHost.Command("PING") + RespTestHost.Command("PING");
        await using var stream = new StalledReplyStream(Encoding.UTF8.GetBytes(requests));
        var writeTimedOut = false;
        try
        {
            await session.RunAsync(stream, cancellationToken, cancellationToken);
        }
        catch (OperationCanceledException error) when (error.CancellationToken.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            writeTimedOut = true;
        }

        _ = await Assert.That(writeTimedOut).IsTrue();
        _ = await Assert.That(stream.WriteCount).IsEqualTo(1);
    }

    private static async Task AssertReplyAsync(SslStream stream, string expected, CancellationToken cancellationToken)
    {
        var bytes = new byte[Encoding.UTF8.GetByteCount(expected)];
        await stream.ReadExactlyAsync(bytes, cancellationToken);
        _ = await Assert.That(Encoding.UTF8.GetString(bytes)).IsEqualTo(expected);
    }

    private static string ExpiredToken(TestJwtCredentials credentials)
    {
        var now = TimeProvider.System.GetUtcNow();
        return CreateToken(credentials, now.AddHours(-2), now.AddHours(-1));
    }

    private static string CreateToken(TestJwtCredentials credentials, DateTimeOffset starts, DateTimeOffset expires)
    {
        var input = TestJwtHelper.CreateSigningInput("HS256", credentials.Issuer, credentials.Audience, starts, expires);
        var signature = HMACSHA256.HashData(credentials.GetSigningKey(), Encoding.ASCII.GetBytes(input));
        return $"{input}.{Base64Url.EncodeToString(signature)}";
    }

    private sealed class ExpireDuringRenewal : IConnectionTokenObserver
    {
        private readonly WallSteppedClock _clock;
        private readonly bool _runExpiryTimer;
        private int _observed;

        internal ExpireDuringRenewal(WallSteppedClock clock, bool runExpiryTimer)
        {
            _clock = clock;
            _runExpiryTimer = runExpiryTimer;
        }

        public ValueTask<ClaimsPrincipal?> OnValidatedAsync(ClaimsPrincipal principal, IServiceProvider services, BinaryConnection? connection, CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref _observed) == 2)
            {
                var step = TimeSpan.FromMinutes(4);
                if (_runExpiryTimer)
                    _clock.Advance(step);
                else
                    _clock.StepWallClock(step);
            }

            return ValueTask.FromResult<ClaimsPrincipal?>(principal);
        }
    }

    private sealed class StalledReplyStream : MemoryStream
    {
        private readonly TaskCompletionSource _reading = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal StalledReplyStream(byte[] requests)
            : base(requests, false)
        {
        }

        internal int WriteCount { get; private set; }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            WriteCount++;
            return new ValueTask(_reading.Task.WaitAsync(cancellationToken));
        }
    }
}
