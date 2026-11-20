using System;
using System.Buffers.Text;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Checks cached discovery reads while an authority refresh is suspended.</summary>
public sealed class JwtMetadataSourceTests
{
    /// <summary>Existing connections retain the published generation throughout a pending discovery refresh.</summary>
    /// <param name="cancellationToken">Test cancellation.</param>
    [Test]
    public async Task CachedReadsContinueDuringRefresh(CancellationToken cancellationToken)
    {
        using var rsa = RSA.Create(2048);
        var key = rsa.ExportParameters(false);
        var modulus = Base64Url.EncodeToString(key.Modulus!);
        var exponent = Base64Url.EncodeToString(key.Exponent!);
        var jwks = $$"""{"keys":[{"kty":"RSA"},{"kty":"RSA","kid":"active","n":"{{modulus}}","e":"{{exponent}}"}]}""";
        using var handler = new DiscoveryHandler(jwks);
        using var client = new HttpClient(handler);
        var source = new JwtMetadataSource("https://issuer.test/discovery", true, client);
        var original = await source.GetConfigurationAsync(cancellationToken);
        _ = await Assert.That(original.SigningKeys.Count).IsEqualTo(1);
        _ = await Assert.That(original.SigningKeys[0].KeyId).IsEqualTo("active");
        source.RequestRefresh();
        var scheduled = source.GetConfigurationAsync(cancellationToken);
        try
        {
            await handler.Started.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
            _ = await Assert.That(scheduled.IsCompletedSuccessfully).IsTrue();
            var cached = source.GetConfigurationAsync(cancellationToken);
            _ = await Assert.That(cached.IsCompletedSuccessfully).IsTrue();
            _ = await Assert.That(await cached).IsSameReferenceAs(original);
            _ = await Assert.That(handler.DiscoveryRequests).IsEqualTo(2);
        }
        finally
        {
            handler.Release();
        }
    }

    /// <summary>HTTPS-only OIDC refuses both redirect and JWKS transport downgrades.</summary>
    /// <param name="redirect">Whether the HTTP endpoint is advertised as a redirect.</param>
    /// <param name="cancellationToken">Test cancellation.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task RejectsMetadataDowngrade(bool redirect, CancellationToken cancellationToken)
    {
        using var handler = new DowngradeHandler(redirect);
        using var client = new HttpClient(handler);
        var source = new JwtMetadataSource("https://issuer.test/discovery", true, client);
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException, JwtMetadata>(new ValueTask<JwtMetadata>(source.GetConfigurationAsync(cancellationToken)), TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        _ = await Assert.That(handler.Requests).IsEqualTo(1);
    }

    private sealed class DowngradeHandler : HttpMessageHandler
    {
        private readonly bool _redirect;

        internal DowngradeHandler(bool redirect)
        {
            _redirect = redirect;
        }

        internal int Requests { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests++;
            if (_redirect)
            {
                var response = new HttpResponseMessage(HttpStatusCode.Found);
                response.Headers.Location = new UriBuilder(Uri.UriSchemeHttp, "issuer.test") { Path = "/discovery" }.Uri;
                return Task.FromResult(response);
            }

            const string discovery = """{"issuer":"issuer","jwks_uri":"http://issuer.test/keys"}""";
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(discovery) });
        }
    }

    private sealed class DiscoveryHandler : HttpMessageHandler
    {
        private readonly TaskCompletionSource _started = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _allowRefresh = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly string _jwks;
        private int _discoveryRequests;

        internal DiscoveryHandler(string jwks)
        {
            _jwks = jwks;
        }

        internal int DiscoveryRequests => Volatile.Read(ref _discoveryRequests);

        internal Task Started => _started.Task;

        internal void Release() => _ = _allowRefresh.TrySetResult();

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var discovery = string.Equals(request.RequestUri!.AbsolutePath, "/discovery", StringComparison.Ordinal);
            if (discovery && Interlocked.Increment(ref _discoveryRequests) == 2)
            {
                _ = _started.TrySetResult();
                await _allowRefresh.Task.WaitAsync(cancellationToken);
            }

            var json = discovery ? """{"issuer":"issuer","jwks_uri":"https://issuer.test/keys"}""" : _jwks;
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(json) };
        }
    }
}
