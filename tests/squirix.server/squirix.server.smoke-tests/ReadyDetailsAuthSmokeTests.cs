using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Networking;
using Xunit;

namespace Squirix.Server.SmokeTests;

/// <summary>
/// Smoke tests verifying JWT auth rules on the <c>/health/ready/details</c> endpoint.
/// </summary>
public sealed class ReadyDetailsAuthSmokeTests : SmokeTestBase
{
    private const string InvalidBearerToken = "invalid.jwt.token";
    private static readonly SocketsHttpHandler RemoteHandler = LoopbackHttp.CreateHandlerAllowingCertificateNameMismatch();
    private static readonly HttpClient RemoteClient = new(RemoteHandler, false);

    /// <summary>
    /// Ensures <c>/health/ready/details</c> follows loopback-anonymous and remote-JWT rules when server auth is configured.
    /// </summary>
    [Fact]
    public async Task ReadyDetailsRejectsMissingValidJwtConfigured()
    {
        var localIp = LocalHostNetworking.GetLocalNonLoopbackIpv4();
        Assert.False(string.IsNullOrWhiteSpace(localIp), "Test requires a non-loopback IPv4 address on the host.");

        var credentials = TestJwtHelper.CreateRandomCredentials();
        var (bindUrl, loopbackUrl) = GetNextAnyInterfaceListenUrls();

        await using var node = await StartNodeAsync(
            bindUrl,
            "node-ready-details-auth",
            new SmokeNodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) },
            cancellationToken: DefaultCancellationToken);

        var loopbackAnonymous = await HttpClient.GetAsync(new Uri(loopbackDetailsUrl), DefaultCancellationToken);
        Assert.True(loopbackAnonymous.IsSuccessStatusCode);

        using (var loopbackAuthorized = new HttpRequestMessage(HttpMethod.Get, loopbackDetailsUrl))
        {
            loopbackAuthorized.Version = HttpVersion.Version20;
            loopbackAuthorized.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
            loopbackAuthorized.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));
            var loopbackWithJwt = await HttpClient.SendAsync(loopbackAuthorized, DefaultCancellationToken);
            Assert.True(loopbackWithJwt.IsSuccessStatusCode);
        }

        var remoteAnonymous = await RemoteClient.GetAsync(new Uri(remoteDetailsUrl), DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.Unauthorized, remoteAnonymous.StatusCode);

        using (var remoteInvalid = new HttpRequestMessage(HttpMethod.Get, remoteDetailsUrl))
        {
            remoteInvalid.Headers.Authorization = new AuthenticationHeaderValue("Bearer", InvalidBearerToken);
            var remoteInvalidJwt = await RemoteClient.SendAsync(remoteInvalid, DefaultCancellationToken);
            Assert.Equal(HttpStatusCode.Unauthorized, remoteInvalidJwt.StatusCode);
        }

        using var remoteValid = new HttpRequestMessage(HttpMethod.Get, remoteDetailsUrl);
        remoteValid.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));
        var remoteWithJwt = await RemoteClient.SendAsync(remoteValid, DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.OK, remoteWithJwt.StatusCode);
    }
}
