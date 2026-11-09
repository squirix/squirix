using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;

namespace Squirix.E2ETests.Fixtures;

/// <summary>Shared startup, routing, and assertion helpers for two-node public API e2e tests.</summary>
internal static class TwoNodeSupport
{
    internal static async Task<Exception?> CaptureAddAsync(ICache<object?> cache, string key, object? value, CancellationToken cancellationToken)
    {
        try
        {
            await cache.AddAsync(key, value, cancellationToken: cancellationToken);
            return null;
        }
        catch (RpcException ex)
        {
            return ex;
        }
        catch (CacheConflictException ex)
        {
            return ex;
        }
        catch (InvalidOperationException ex)
        {
            return ex;
        }
        catch (IOException ex)
        {
            return ex;
        }
    }

    internal static string FindKeyOwnedBy(string cacheName, string ownerId, string prefix) => KeyOwnerHelper.TwoNode.FindKeyOwnedBy(cacheName, ownerId, prefix);
}
