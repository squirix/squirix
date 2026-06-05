using System.Buffers;
using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Squirix.Server.Core;
using Squirix.Server.Utils;

namespace Squirix.Server.Adapters.Rest;

/// <summary>
/// Shared helper methods used by REST endpoint mappings.
/// </summary>
internal static partial class HttpEndpoints
{
    /// <summary>
    /// Writes a structured log entry when admin endpoints are intentionally not exposed.
    /// </summary>
    /// <param name="logger">Destination logger.</param>
    /// <param name="environment">Current host environment name.</param>
    /// <param name="flagState">Raw value of the admin exposure flag.</param>
    [LoggerMessage(
        EventId = 1002,
        Level = LogLevel.Information,
        Message = "Skipping /admin endpoints because the current environment \"{environment}\" is not permitted (flag: {flagState}).")]
    internal static partial void LogAdminEndpointsSkipped(ILogger logger, string environment, string flagState);

    internal static bool ShouldExposeAdminEndpoints(IHostEnvironment environment, out string? flagValue)
    {
        if (!environment.IsDevelopment())
            return EnvVariables.ReadBool("SQUIRIX_ADMIN_ENABLED", out flagValue);
        flagValue = null;
        return true;
    }

    internal static IResult? ValidateContentLength(HttpRequest request, int maxBytes) =>
        request.ContentLength.HasValue && request.ContentLength.Value > maxBytes ? PayloadTooLarge(maxBytes) : null;

    internal static IResult? ValidateEntry<T>(CacheEntry<T> entry, JsonSerializerOptions serializerOptions, int maxBytes)
    {
        var writer = new ArrayBufferWriter<byte>();
        using (var jsonWriter = new Utf8JsonWriter(writer))
        {
            JsonSerializer.Serialize(jsonWriter, entry, serializerOptions);
        }

        return writer.WrittenCount > maxBytes ? PayloadTooLarge(maxBytes) : null;
    }

    internal static IResult? ValidateKey(string key) => CacheKeyValidator.TryValidate(key, out _) ? null : CacheContractHttpResults.InvalidCacheKey(key);

    private static IResult PayloadTooLarge(int maxBytes) => CacheContractHttpResults.PayloadTooLarge(maxBytes);
}
