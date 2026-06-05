using Microsoft.AspNetCore.Http;
using Squirix.Server.Errors;

namespace Squirix.Server.Adapters.Rest;

/// <summary>
/// Centralized HTTP projections for <see cref="CacheOperationContract" /> outcomes used by REST endpoints.
/// </summary>
internal static class CacheContractHttpResults
{
    public static IResult InvalidCacheKey(string? key) => CacheOperationContract.InvalidCacheKey(key).ToHttpResult();

    public static IResult NotFound() => CacheOperationContract.NotFound().ToHttpResult();

    public static IResult PayloadTooLarge(int maxBytes) => CacheOperationContract.PayloadTooLarge(maxBytes).ToHttpResult();
}
