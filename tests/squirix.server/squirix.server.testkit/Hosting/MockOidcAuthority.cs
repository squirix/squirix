using System;
using System.Buffers.Text;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit.Networking;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>In-process OIDC authority that serves discovery metadata and JWKS for JWT bearer tests.</summary>
[Immutable]
public sealed class MockOidcAuthority : IAsyncDisposable
{
    private const string RsaSigningAlgorithm = "RS256";

    private readonly CancellationTokenSource _stopping = new();
    private readonly TcpListener _listener;
    private readonly HeldPort _heldPort;
    private readonly Task _serveTask;
    private readonly string _keyId;

    private readonly RSA _signingKey;

    private MockOidcAuthority(TcpListener listener, HeldPort heldPort, string authority, RSA signingKey, string keyId, byte[] discovery, byte[] jwks)
    {
        _listener = listener;
        _heldPort = heldPort;
        AuthorityEndpoint = authority;
        Issuer = authority;
        _signingKey = signingKey;
        _keyId = keyId;
        _serveTask = ServeAsync(discovery, jwks, _stopping.Token);
    }

    private static ListenPortPool PortPool { get; } = ConsumerPortSlicer.PoolFor(HostPortRegion.MockOidcAuthority);

    /// <summary>Gets the authority base endpoint (also used as the token issuer).</summary>
    private string AuthorityEndpoint { get; }

    /// <summary>Gets the issuer claim value published in discovery metadata.</summary>
    private string Issuer { get; }

    /// <summary>Starts a loopback mock authority on an ephemeral HTTP port.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A started mock authority.</returns>
    public static Task<MockOidcAuthority> StartAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var held = PortPool.HoldPort();
        RSA? signingKey = null;
        TcpListener? listener = null;
        try
        {
            var authority = NodeInvariantIndexStrings.FormatOrigin("http", "127.0.0.1", held.Port);
            signingKey = RSA.Create(2048);
            const string keyId = "mock-oidc-key";
            var discovery = new OidcDiscoveryDocument
            {
                Issuer = authority,
                JwksEndpoint = $"{authority}/.well-known/jwks",
            };
            var discoveryBytes = JsonSerializer.SerializeToUtf8Bytes(discovery, MockOidcJsonContext.Default.OidcDiscoveryDocument);
            var jwksBytes = CreateJwks(signingKey.ExportParameters(false), keyId);

            listener = new TcpListener(IPAddress.Loopback, held.Port);
            held.Dispose();
            cancellationToken.ThrowIfCancellationRequested();
            listener.Start();
            return Task.FromResult(new MockOidcAuthority(listener, held, authority, signingKey, keyId, discoveryBytes, jwksBytes));
        }
        catch
        {
            listener?.Dispose();
            signingKey?.Dispose();
            held.Return();
            throw;
        }
    }

    /// <summary>Issues a bearer token signed with the authority's RSA key.</summary>
    /// <param name="audience">JWT audience claim.</param>
    /// <param name="lifetime">Optional token lifetime; defaults to five minutes.</param>
    /// <param name="issuer">Optional issuer claim; defaults to the discovery document issuer.</param>
    /// <returns>A compact JWT bearer token string.</returns>
    public string CreateBearerToken(string audience, TimeSpan? lifetime = null, string? issuer = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(audience);

        var now = DateTimeOffset.UtcNow;
        var effectiveLifetime = lifetime ?? TimeSpan.FromMinutes(5);
        var expires = now.Add(effectiveLifetime);
        var notBefore = effectiveLifetime < TimeSpan.Zero ? expires.AddMinutes(-1) : now.AddMinutes(-1);
        var input = TestJwtHelper.CreateSigningInput(RsaSigningAlgorithm, issuer ?? Issuer, audience, notBefore, expires, keyId: _keyId);
        var signature = _signingKey.SignData(Encoding.ASCII.GetBytes(input), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        return $"{input}.{Base64Url.EncodeToString(signature)}";
    }

    /// <summary>Maps the mock authority to per-node security options.</summary>
    /// <param name="audience">JWT audience validation value.</param>
    /// <returns>Per-node security override for in-process test hosts.</returns>
    public TestNodeSecurityOptions ToSecurityOptions(string audience) => new()
    {
        JwtAuthority = AuthorityEndpoint,
        JwtIssuer = Issuer,
        JwtAudience = audience,
        JwtAllowHttpMetadata = true,
    };

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        try
        {
            await _stopping.CancelAsync().ConfigureAwait(false);
            await _serveTask.ConfigureAwait(false);
        }
        finally
        {
            _listener.Dispose();
            _stopping.Dispose();
            _signingKey.Dispose();
            _heldPort.Return();
        }
    }

    private static byte[] CreateJwks(RSAParameters key, string keyId)
    {
        var signingKey = new JsonWebKey(
            RsaSigningAlgorithm,
            Base64Url.EncodeToString(key.Exponent),
            keyId,
            "RSA",
            Base64Url.EncodeToString(key.Modulus),
            "sig");
        return JsonSerializer.SerializeToUtf8Bytes(new JwksDocument([signingKey]), MockOidcJsonContext.Default.JwksDocument);
    }

    private static async Task ReplyAsync(NetworkStream stream, byte[] discovery, byte[] jwks, CancellationToken stopping)
    {
        var buffer = new byte[4096];
        var received = 0;
        while (received < buffer.Length)
        {
            var count = await stream.ReadAsync(buffer.AsMemory(received), stopping).ConfigureAwait(false);
            if (count == 0)
                return;
            received += count;
            if (buffer.AsSpan(0, received).IndexOf("\r\n\r\n"u8) >= 0)
                break;
        }

        var payload = SelectPayload(buffer.AsSpan(0, received), discovery, jwks);
        var status = payload == null ? "404 Not Found" : "200 OK";
        var header = string.Create(
            CultureInfo.InvariantCulture,
            $"HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {payload?.Length ?? 0}\r\nConnection: close\r\n\r\n");
        await stream.WriteAsync(Encoding.ASCII.GetBytes(header), stopping).ConfigureAwait(false);
        if (payload != null)
            await stream.WriteAsync(payload, stopping).ConfigureAwait(false);
    }

    private static byte[]? SelectPayload(ReadOnlySpan<byte> request, byte[] discovery, byte[] jwks)
    {
        var firstLine = request.IndexOf("\r\n"u8);
        byte[]? payload = null;
        if (firstLine >= 0)
        {
            var start = request[..firstLine];
            if (start.StartsWith("GET /.well-known/openid-configuration HTTP/1."u8))
                payload = discovery;
            else if (start.StartsWith("GET /.well-known/jwks HTTP/1."u8))
                payload = jwks;
        }

        return payload;
    }

    private async Task ServeAsync(byte[] discovery, byte[] jwks, CancellationToken stopping)
    {
        try
        {
            while (!stopping.IsCancellationRequested)
            {
                using var client = await _listener.AcceptTcpClientAsync(stopping).ConfigureAwait(false);
                var stream = client.GetStream();
                try
                {
                    await ReplyAsync(stream, discovery, jwks, stopping).ConfigureAwait(false);
                }
                catch (IOException)
                {
                    // The HTTP client may abandon a metadata request while the fixture is writing its reply.
                }
            }
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested)
        {
            // Fixture disposal cancels accept and metadata reads.
        }
    }

    internal sealed record JwksDocument(JsonWebKey[] Keys);

    internal sealed record JsonWebKey(string Alg, string E, string Kid, string Kty, string N, string Use);
}
