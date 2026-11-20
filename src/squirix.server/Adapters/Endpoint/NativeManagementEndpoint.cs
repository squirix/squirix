using System;
using System.Buffers;
using System.Globalization;
using System.IO;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Squirix.Server.Adapters.Rest;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.Runtime.Diagnostics;

namespace Squirix.Server.Adapters.Endpoint;

internal sealed class NativeManagementEndpoint
{
    private const int MaxHeaderBytes = 16 * 1024;
    private const string JsonContentType = "application/json; charset=utf-8";
    private const string TextContentType = "text/plain";

    private static readonly ReadOnlyMemory<byte> CompatibilityHealthBody = Encoding.UTF8.GetBytes("\"OK\"");
    private static readonly ReadOnlyMemory<byte> HealthyBody = Encoding.UTF8.GetBytes(nameof(HealthStatus.Healthy));
    private static readonly ReadOnlyMemory<byte> DegradedBody = Encoding.UTF8.GetBytes(nameof(HealthStatus.Degraded));
    private static readonly ReadOnlyMemory<byte> UnhealthyBody = Encoding.UTF8.GetBytes(nameof(HealthStatus.Unhealthy));

    private static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(10);

    private readonly IServiceProvider _services;
    private readonly Func<string, CancellationToken, ValueTask<bool>> _authenticateBearer;

    internal NativeManagementEndpoint(IServiceProvider services, Func<string, CancellationToken, ValueTask<bool>> authenticateBearer)
    {
        _services = services;
        _authenticateBearer = authenticateBearer;
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
        var response = await ExecuteAsync(request, remote.Address, token).ConfigureAwait(false);
        await WriteAsync(stream, response, request.Head, token).ConfigureAwait(false);
    }

    internal ValueTask<bool> CanAccessDetailsAsync(IPAddress? remote, string? bearer, CancellationToken cancellationToken)
    {
        if (remote?.IsIPv4MappedToIPv6 == true)
            remote = remote.MapToIPv4();
        var loopback = remote != null && IPAddress.IsLoopback(remote);
        return !loopback && !string.IsNullOrEmpty(bearer) ? _authenticateBearer(bearer, cancellationToken) : ValueTask.FromResult(loopback);
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
                total += read;
                var end = buffer.AsSpan(0, total).IndexOf("\r\n\r\n"u8);
                var headerSize = end >= 0 ? end : total;
                if (HasInvalidHeaderBytes(buffer.AsSpan(0, headerSize)))
                    return (null, Response.Empty("400 Bad Request"));
                if (end >= 0)
                    return ParseRequest(Encoding.ASCII.GetString(buffer, 0, end));
            }

            return (null, Response.Empty("431 Request Header Fields Too Large"));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer, true);
        }
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

    private static (Request? Request, Response? Error) ParseRequest(string headers)
    {
        var lines = headers.Split("\r\n");
        var start = lines[0].Split(' ');
        var badRequest = Response.Empty("400 Bad Request");
        if (start.Length != 3 || start[2] is not ("HTTP/1.1" or "HTTP/1.0") || !start[1].StartsWith('/'))
            return (null, badRequest);

        var head = string.Equals(start[0], "HEAD", StringComparison.Ordinal);
        if (!head && !string.Equals(start[0], "GET", StringComparison.Ordinal))
            return (null, Response.Empty("405 Method Not Allowed"));

        if (!TryReadHeaders(lines, out var bearer))
            return (null, badRequest);

        var query = start[1].IndexOf('?', StringComparison.Ordinal);
        var path = query < 0 ? start[1] : start[1][..query];
        return (new Request(path, bearer, head), null);
    }

    private static bool TryReadHeaders(string[] lines, out string? bearer)
    {
        bearer = null;
        var authorizationSeen = false;
        for (var i = 1; i < lines.Length; i++)
        {
            var line = lines[i].AsSpan();
            var colon = line.IndexOf(':');
            if (colon <= 0 || IsInvalidHeaderName(line[..colon]))
                return false;

            var name = line[..colon];
            var value = line[(colon + 1)..].Trim();
            if (HasUnsupportedBody(name, value))
                return false;

            if (!name.Equals("Authorization", StringComparison.OrdinalIgnoreCase))
                continue;
            if (authorizationSeen)
                return false;
            authorizationSeen = true;

            if (value.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
                bearer = value["Bearer ".Length..].Trim().ToString();
        }

        return true;
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

    private static async Task WriteAsync(Stream stream, Response response, bool head, CancellationToken cancellationToken)
    {
        var type = response.ContentType == null ? string.Empty : $"Content-Type: {response.ContentType}\r\n";
        var cache = response.NoCache ? "Cache-Control: no-store, no-cache\r\nPragma: no-cache\r\nExpires: Thu, 01 Jan 1970 00:00:00 GMT\r\n" : string.Empty;
        var header = string.Create(
            CultureInfo.InvariantCulture,
            $"HTTP/1.1 {response.Status}\r\n{type}{cache}Content-Length: {response.Body.Length}\r\nConnection: close\r\n\r\n");
        await stream.WriteAsync(Encoding.ASCII.GetBytes(header), cancellationToken).ConfigureAwait(false);
        if (!head && !response.Body.IsEmpty)
            await stream.WriteAsync(response.Body, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<Response> ExecuteAsync(Request request, IPAddress remote, CancellationToken cancellationToken)
    {
        if (string.Equals(request.Path, "/health", StringComparison.OrdinalIgnoreCase))
            return new Response("200 OK", JsonContentType, CompatibilityHealthBody, false);

        if (string.Equals(request.Path, "/health/live", StringComparison.OrdinalIgnoreCase))
            return await CheckHealthAsync("live", cancellationToken).ConfigureAwait(false);
        if (string.Equals(request.Path, "/health/ready", StringComparison.OrdinalIgnoreCase))
            return await CheckHealthAsync("ready", cancellationToken).ConfigureAwait(false);

        if (string.Equals(request.Path, "/health/ready/details", StringComparison.OrdinalIgnoreCase))
        {
            if (!await CanAccessDetailsAsync(remote, request.Bearer, cancellationToken).ConfigureAwait(false))
                return Response.Empty("401 Unauthorized");

            var provider = _services.GetRequiredService<IHealthReadyDetailsProvider>();
            var snapshot = await provider.GetSnapshotAsync(cancellationToken).ConfigureAwait(false);
            var details = HealthDetailsResponseMapper.Build(snapshot);
            var body = JsonSerializer.SerializeToUtf8Bytes(details, RestJsonSerializerContext.Default.HealthReadyDetailsResponse);
            return new Response("200 OK", JsonContentType, body, false);
        }

        var metrics = _services.GetRequiredService<IOptions<PrometheusMetricsEndpointOptions>>().Value;
        var response = Response.Empty("404 Not Found");
        if (metrics.Enabled && string.Equals(metrics.Path, request.Path, StringComparison.OrdinalIgnoreCase) &&
            _services.GetService<PrometheusMetricsScraper>() is { } scraper)
        {
            response = await CanAccessDetailsAsync(remote, request.Bearer, cancellationToken).ConfigureAwait(false)
                ? new Response("200 OK", "text/plain; version=0.0.4", Encoding.UTF8.GetBytes(scraper.Scrape()), false)
                : Response.Empty("401 Unauthorized");
        }

        return response;
    }

    private async ValueTask<Response> CheckHealthAsync(string tag, CancellationToken cancellationToken)
    {
        var report = await _services.GetRequiredService<HealthCheckService>().CheckHealthAsync(
            registration => registration.Tags.Contains(tag), cancellationToken).ConfigureAwait(false);
        return report.Status switch
        {
            HealthStatus.Healthy => new Response("200 OK", TextContentType, HealthyBody, true),
            HealthStatus.Degraded => new Response("200 OK", TextContentType, DegradedBody, true),
            _ => new Response("503 Service Unavailable", TextContentType, UnhealthyBody, true),
        };
    }

    private readonly record struct Request(string Path, string? Bearer, bool Head);

    private readonly record struct Response(string Status, string? ContentType, ReadOnlyMemory<byte> Body, bool NoCache)
    {
        internal static Response Empty(string status) => new(status, null, ReadOnlyMemory<byte>.Empty, false);
    }
}
