using System;
using System.Buffers;
using System.Collections.Frozen;
using System.Globalization;
using System.IO;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Rest;
using Squirix.Server.Composition;
using Squirix.Server.Logging;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.Runtime.Diagnostics;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Serves health and metrics requests on the dedicated management listener.</summary>
internal sealed class NativeManagementEndpoint
{
    internal static readonly FrozenSet<string> Probes = new[] { "/health", "/health/live", "/health/ready", "/health/ready/details" }.ToFrozenSet(StringComparer.OrdinalIgnoreCase);
    private const string JsonContentType = "application/json; charset=utf-8";

    private const string OkStatus = "200 OK";

    private const int MaxHeaderBytes = 16 * 1024;
    private const string TextContentType = "text/plain";

    private static readonly ReadOnlyMemory<byte> CompatibilityHealthBody = Encoding.UTF8.GetBytes("\"OK\"");
    private static readonly ReadOnlyMemory<byte> DegradedBody = Encoding.UTF8.GetBytes(nameof(ReadinessStatus.Degraded));
    private static readonly ReadOnlyMemory<byte> HealthyBody = Encoding.UTF8.GetBytes(nameof(ReadinessStatus.Healthy));

    private static readonly SearchValues<char> InvalidHostCharacters = SearchValues.Create(" \t\r\n/\\?#@");

    private static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(10);
    private static readonly ReadOnlyMemory<byte> UnhealthyBody = Encoding.UTF8.GetBytes(nameof(ReadinessStatus.Unhealthy));
    private readonly Func<string, CancellationToken, ValueTask<bool>> _authenticateBearer;

    private readonly IServerLogger<NativeManagementEndpoint> _logger;
    private readonly IServiceProvider _services;

    internal NativeManagementEndpoint(IServiceProvider services, Func<string, CancellationToken, ValueTask<bool>> authenticateBearer, IServerLogger<NativeManagementEndpoint> logger)
    {
        _services = services;
        _logger = logger;
        _authenticateBearer = authenticateBearer;
    }

    internal ValueTask<bool> CanAccessDetailsAsync(IPAddress? remote, string? bearer, CancellationToken cancellationToken)
    {
        if (remote?.IsIPv4MappedToIPv6 == true)
            remote = remote.MapToIPv4();
        var loopback = remote != null && IPAddress.IsLoopback(remote);
        return !loopback && !string.IsNullOrEmpty(bearer) ? _authenticateBearer(bearer, cancellationToken) : ValueTask.FromResult(loopback);
    }

    internal async Task ServeAsync(Stream stream, IPEndPoint remote, CancellationToken graceful, CancellationToken abort)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(graceful, abort);
        timeout.CancelAfter(RequestTimeout);
        var token = timeout.Token;
        var parsed = await ReadRequestAsync(stream, token).ConfigureAwait(false);
        if (parsed.Error is { } error)
        {
            await WriteAsync(stream, error, false, token).ConfigureAwait(false);
            return;
        }

        if (parsed.Request is not { } request)
            return;
        LogRequest(_logger, request.Path);
        var response = await ExecuteAsync(request, remote.Address, token).ConfigureAwait(false);
        await WriteAsync(stream, response, request.Head, token).ConfigureAwait(false);
    }

    private static bool HasInvalidHeaderBytes(ReadOnlySpan<byte> bytes)
    {
        foreach (var value in bytes)
        {
            if (value > 126 || (value < 32 && value is not (9 or 10 or 13)))
                return true;
        }

        return false;
    }

    private static bool HasUnsupportedBody(ReadOnlySpan<char> name, ReadOnlySpan<char> value) =>
        name.Equals("Transfer-Encoding", StringComparison.OrdinalIgnoreCase) ||
        (name.Equals("Content-Length", StringComparison.OrdinalIgnoreCase) && value is not "0");

    private static bool IsInvalidHeaderName(ReadOnlySpan<char> name)
    {
        foreach (var character in name)
        {
            if (!char.IsAsciiLetterOrDigit(character) && !"!#$%&'*+-.^_`|~".Contains(character, StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    private static void LogRequest(IServerLogger logger, string path)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.LogRequestEvent.Write(logger, ServerLogLevel.Information, null, ValueTuple.Create(path));
    }

    private static bool MatchesMetricsPath(string configuredPath, string requestPath) =>
        string.Equals(configuredPath, requestPath, StringComparison.OrdinalIgnoreCase) ||
        (requestPath.Contains('%', StringComparison.Ordinal) &&
         string.Equals(configuredPath, Uri.UnescapeDataString(requestPath), StringComparison.OrdinalIgnoreCase));

    private static (Request? Request, Response? Error) ParseRequest(string headers)
    {
        var lines = headers.Split("\r\n");
        var start = lines[0].Split(' ');
        var badRequest = Response.Empty("400 Bad Request");
        if (start.Length != 3 || start[2] is not ("HTTP/1.1" or "HTTP/1.0") || start[1] is not ['/', ..])
            return (null, badRequest);

        var head = string.Equals(start[0], "HEAD", StringComparison.Ordinal);
        var supportedMethod = head || string.Equals(start[0], "GET", StringComparison.Ordinal);
        if (!TryReadHeaders(lines, string.Equals(start[2], "HTTP/1.1", StringComparison.Ordinal), supportedMethod, out var bearer))
            return (null, badRequest);

        if (!supportedMethod)
            return (null, Response.Empty("405 Method Not Allowed"));

        var query = start[1].IndexOf('?', StringComparison.Ordinal);
        var path = query < 0 ? start[1] : start[1][..query];
        return (new Request(path, bearer, head), null);
    }

    private static async Task<(Request? Request, Response? Error)> ReadRequestAsync(Stream stream, CancellationToken cancellationToken)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(MaxHeaderBytes);
        try
        {
            var total = 0;
            while (total < MaxHeaderBytes)
            {
                var read = await stream.ReadAsync(buffer.AsMemory(total, MaxHeaderBytes - total), cancellationToken).ConfigureAwait(false);
                if (read == 0)
                    return (null, null);
                var previous = total;
                total += read;
                var searchStart = Math.Max(0, previous - 3);
                var delimiter = buffer.AsSpan(searchStart, total - searchStart).IndexOf("\r\n\r\n"u8);
                var headerSize = delimiter >= 0 ? searchStart + delimiter : total;
                if (headerSize > previous && HasInvalidHeaderBytes(buffer.AsSpan(previous, headerSize - previous)))
                    return (null, Response.Empty("400 Bad Request"));
                if (delimiter >= 0)
                    return ParseRequest(Encoding.ASCII.GetString(buffer, 0, headerSize));
            }

            return (null, Response.Empty("431 Request Header Fields Too Large"));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer, true);
        }
    }

    private static bool TryReadHeaders(string[] lines, bool requireHost, bool rejectBody, out string? bearer)
    {
        bearer = null;
        var authorizationSeen = false;
        var hostSeen = false;
        for (var i = 1; i < lines.Length; i++)
        {
            var line = lines[i].AsSpan();
            var colon = line.IndexOf(':');
            if (colon <= 0 || IsInvalidHeaderName(line[..colon]))
                return false;

            var name = line[..colon];
            var value = line[(colon + 1)..].Trim();
            if (!TryValidateHeader(name, value, rejectBody, ref hostSeen))
                return false;

            if (!name.Equals("Authorization", StringComparison.OrdinalIgnoreCase))
                continue;
            if (authorizationSeen)
                return false;
            authorizationSeen = true;

            if (value.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
                bearer = value["Bearer ".Length..].Trim().ToString();
        }

        return hostSeen || !requireHost;
    }

    private static bool TryValidateHeader(ReadOnlySpan<char> name, ReadOnlySpan<char> value, bool rejectBody, ref bool hostSeen)
    {
        if (rejectBody && HasUnsupportedBody(name, value))
            return false;
        if (!name.Equals("Host", StringComparison.OrdinalIgnoreCase))
            return true;
        if (hostSeen || value.IsEmpty || value.IndexOfAny(InvalidHostCharacters) >= 0 ||
            !Uri.TryCreate($"https://{value}", UriKind.Absolute, out var authority) || authority.HostNameType == UriHostNameType.Unknown)
            return false;

        hostSeen = true;
        return true;
    }

    private static async Task WriteAsync(Stream stream, Response response, bool head, CancellationToken cancellationToken)
    {
        var type = response.ContentType == null ? string.Empty : $"Content-Type: {response.ContentType}\r\n";
        var cache = response.NoCache ? "Cache-Control: no-store, no-cache\r\nPragma: no-cache\r\nExpires: Thu, 01 Jan 1970 00:00:00 GMT\r\n" : string.Empty;
        var policy = response.Status switch
        {
            "401 Unauthorized" => "WWW-Authenticate: Bearer realm=\"management\"\r\n",
            "405 Method Not Allowed" => "Allow: GET, HEAD\r\n",
            _ => string.Empty,
        };
        var header = string.Create(
            CultureInfo.InvariantCulture,
            $"HTTP/1.1 {response.Status}\r\n{type}{cache}{policy}Content-Length: {response.Body.Length}\r\nConnection: close\r\n\r\n");
        await stream.WriteAsync(Encoding.ASCII.GetBytes(header), cancellationToken).ConfigureAwait(false);
        if (!head && !response.Body.IsEmpty)
            await stream.WriteAsync(response.Body, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<Response> CheckReadinessAsync(CancellationToken cancellationToken)
    {
        var status = await _services.Require<ReadinessMonitor>().GetStatusAsync(cancellationToken).ConfigureAwait(false);
        return status switch
        {
            ReadinessStatus.Healthy => new Response(OkStatus, TextContentType, HealthyBody, true),
            ReadinessStatus.Degraded => new Response(OkStatus, TextContentType, DegradedBody, true),
            _ => new Response("503 Service Unavailable", TextContentType, UnhealthyBody, true),
        };
    }

    private async ValueTask<Response> ExecuteAsync(Request request, IPAddress remote, CancellationToken cancellationToken)
    {
        _ = Probes.TryGetValue(request.Path, out var probe);
        if (string.Equals(probe, "/health", StringComparison.Ordinal))
            return new Response(OkStatus, JsonContentType, CompatibilityHealthBody, false);

        if (string.Equals(probe, "/health/live", StringComparison.Ordinal))
            return new Response(OkStatus, TextContentType, HealthyBody, true);
        if (string.Equals(probe, "/health/ready", StringComparison.Ordinal))
            return await CheckReadinessAsync(cancellationToken).ConfigureAwait(false);

        if (string.Equals(probe, "/health/ready/details", StringComparison.Ordinal))
        {
            if (!await CanAccessDetailsAsync(remote, request.Bearer, cancellationToken).ConfigureAwait(false))
                return Response.Empty("401 Unauthorized");

            var provider = _services.Require<IHealthReadyDetailsProvider>();
            var snapshot = await provider.GetSnapshotAsync(cancellationToken).ConfigureAwait(false);
            var details = HealthDetailsResponseMapper.Build(snapshot);
            var body = JsonSerializer.SerializeToUtf8Bytes(details, RestJsonSerializerContext.Default.HealthReadyDetailsResponse);
            return new Response(OkStatus, JsonContentType, body, false);
        }

        var metrics = _services.Require<PrometheusMetricsEndpointOptions>();
        var response = Response.Empty("404 Not Found");
        if (metrics.Enabled && MatchesMetricsPath(metrics.Path, request.Path) &&
            _services.Find<PrometheusMetricsScraper>() is { } scraper)
        {
            response = await CanAccessDetailsAsync(remote, request.Bearer, cancellationToken).ConfigureAwait(false)
                ? new Response(OkStatus, "text/plain; version=0.0.4", Encoding.UTF8.GetBytes(scraper.Scrape()), false)
                : Response.Empty("401 Unauthorized");
        }

        return response;
    }

    private readonly record struct Request(string Path, string? Bearer, bool Head);

    private readonly record struct Response(string Status, string? ContentType, ReadOnlyMemory<byte> Body, bool NoCache)
    {
        internal static Response Empty(string status) => new(status, null, ReadOnlyMemory<byte>.Empty, false);
    }

    private static class Events
    {
        internal static readonly LogEvent LogRequestEvent = new(3035, nameof(LogRequest), "Management request {Path}");
    }
}
