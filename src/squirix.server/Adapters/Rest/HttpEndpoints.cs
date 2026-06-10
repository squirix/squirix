using System.Buffers;
using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Squirix.Server.Core;

namespace Squirix.Server.Adapters.Rest;

/// <summary>
/// Shared helper methods used by REST endpoint mappings.
/// </summary>
internal static class HttpEndpoints
{
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
