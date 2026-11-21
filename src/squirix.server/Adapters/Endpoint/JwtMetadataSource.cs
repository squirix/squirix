using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Adapters.Endpoint;

internal sealed class JwtMetadataSource : IJwtMetadataSource
{
    private static readonly HttpClient Client = new(new SocketsHttpHandler
    {
        AllowAutoRedirect = false,
        UseCookies = false,
        PooledConnectionLifetime = TimeSpan.FromMinutes(5),
    });

    private readonly Lock _gate = new();
    private readonly HttpClient _client;
    private readonly bool _requireHttps;
    private readonly Uri _discovery;
    private Task<JwtMetadata>? _inFlight;
    private JwtMetadata? _current;
    private long _nextAutomatic;
    private long _nextRequested;
    private int _refreshRequested;

    internal JwtMetadataSource(string discoveryUrl, bool requireHttps)
        : this(discoveryUrl, requireHttps, Client)
    {
    }

    internal JwtMetadataSource(string discoveryUrl, bool requireHttps, HttpClient client)
    {
        _client = client;
        _requireHttps = requireHttps;
        _discovery = ValidateUri(new Uri(discoveryUrl, UriKind.Absolute));
    }

    public TimeSpan LastKnownGoodLifetime { get; internal set; } = TimeSpan.FromHours(1);

    public bool UseLastKnownGoodConfiguration { get; internal set; } = true;

    internal string MetadataAddress => _discovery.AbsoluteUri;

    public void RequestRefresh() => Volatile.Write(ref _refreshRequested, 1);

    public Task<JwtMetadata> GetConfigurationAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var current = Volatile.Read(ref _current);
        if (current != null && !IsDue())
            return Task.FromResult(current);

        lock (_gate)
        {
            current = _current;
            if ((current == null || IsDue()) && (_inFlight?.IsCompleted != false))
            {
                Volatile.Write(ref _refreshRequested, 0);
                _inFlight = RefreshAsync(current);
            }

            return current == null ? _inFlight!.WaitAsync(cancellationToken) : Task.FromResult(current);
        }
    }

    private async Task<JwtMetadata> RefreshAsync(JwtMetadata? previous)
    {
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var updated = await FetchAsync(timeout.Token).ConfigureAwait(false);
            var now = Environment.TickCount64;
            Volatile.Write(ref _nextAutomatic, now + (TimeSpan.FromHours(12).Ticks / TimeSpan.TicksPerMillisecond));
            Volatile.Write(ref _nextRequested, previous == null ? now : now + (TimeSpan.FromMinutes(5).Ticks / TimeSpan.TicksPerMillisecond));
            Volatile.Write(ref _current, updated);
            return updated;
        }
        catch (Exception ex) when (previous != null && ex is HttpRequestException or IOException or OperationCanceledException or JsonException or InvalidDataException or FormatException or System.Security.Cryptography.CryptographicException or ArgumentException or InvalidOperationException or System.Collections.Generic.KeyNotFoundException)
        {
            var retry = Environment.TickCount64 + (TimeSpan.FromMinutes(5).Ticks / TimeSpan.TicksPerMillisecond);
            Volatile.Write(ref _nextAutomatic, retry);
            Volatile.Write(ref _nextRequested, retry);
            return previous;
        }
    }

    private bool IsDue()
    {
        var now = Environment.TickCount64;
        return now >= Volatile.Read(ref _nextAutomatic) || (Volatile.Read(ref _refreshRequested) != 0 && now >= Volatile.Read(ref _nextRequested));
    }

    private async Task<JwtMetadata> FetchAsync(CancellationToken cancellationToken)
    {
        using var discovery = await DownloadAsync(_discovery, cancellationToken).ConfigureAwait(false);
        var issuer = discovery.RootElement.GetProperty("issuer").GetString();
        if (string.IsNullOrWhiteSpace(issuer))
            throw new InvalidDataException("OIDC discovery requires an issuer.");
        var location = ValidateUri(new Uri(discovery.RootElement.GetProperty("jwks_uri").GetString()!, UriKind.Absolute));
        using var jwks = await DownloadAsync(location, cancellationToken).ConfigureAwait(false);
        var result = new JwtMetadata { Issuer = issuer };
        var keys = jwks.RootElement.GetProperty("keys");
        if (keys.GetArrayLength() > 256)
            throw new InvalidDataException("OIDC signing key set exceeds the supported size.");
        foreach (var item in keys.EnumerateArray())
        {
            if (JwtVerificationKey.FromJwk(item) is { } key)
                result.SigningKeys.Add(key);
        }

        return result;
    }

    private Uri ValidateUri(Uri uri) =>
        (string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.Ordinal) || (!_requireHttps && string.Equals(uri.Scheme, Uri.UriSchemeHttp, StringComparison.Ordinal))) && uri.UserInfo.Length == 0
            ? uri
            : throw new InvalidDataException("OIDC metadata endpoint has an unsupported URI.");

    private async Task<JsonDocument> DownloadAsync(Uri location, CancellationToken cancellationToken)
    {
        for (var redirect = 0; redirect <= 8; redirect++)
        {
            using var response = await _client.GetAsync(location, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
            if (response.StatusCode is HttpStatusCode.MovedPermanently or HttpStatusCode.Found or HttpStatusCode.SeeOther or HttpStatusCode.TemporaryRedirect or HttpStatusCode.PermanentRedirect &&
                response.Headers.Location is { } next)
            {
                location = ValidateUri(new Uri(location, next));
                continue;
            }

            _ = response.EnsureSuccessStatusCode();
            const int maximum = 1024 * 1024;
            await response.Content.LoadIntoBufferAsync(maximum, cancellationToken).ConfigureAwait(false);
            return JsonDocument.Parse(await response.Content.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false), new JsonDocumentOptions { AllowDuplicateProperties = false });
        }

        throw new HttpRequestException("Too many redirects for OIDC metadata.");
    }
}
