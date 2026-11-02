using System;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.DockerClient;

internal static class FailoverDemo
{
    private const int KeyCount = 96;

    internal static async Task RunAsync(ICache<string> cache, CancellationToken cancellationToken)
    {
        var prefix = Guid.NewGuid().ToString("N") + "-";
        for (var index = 0; index < KeyCount; index++)
        {
            var suffix = index.ToString(CultureInfo.InvariantCulture);
            var key = prefix + suffix;
            var value = "before-" + suffix;
            await cache.SetAsync(key, value, cancellationToken: cancellationToken).ConfigureAwait(false);
            await VerifyAsync(cache, key, value, cancellationToken).ConfigureAwait(false);
        }

        Console.WriteLine($"READY: {KeyCount} values written and verified; waiting for leader termination");
        var signal = await Console.In.ReadLineAsync(cancellationToken).ConfigureAwait(false);
        if (!string.Equals(signal, "continue", StringComparison.Ordinal))
            throw new InvalidOperationException("Expected the failover controller to send continue.");

        var elapsed = Stopwatch.StartNew();
        for (var index = 0; index < KeyCount; index++)
        {
            var suffix = index.ToString(CultureInfo.InvariantCulture);
            var key = prefix + suffix;
            await VerifyAsync(cache, key, "before-" + suffix, cancellationToken).ConfigureAwait(false);
            var value = "after-" + suffix;
            await cache.SetAsync(key, value, cancellationToken: cancellationToken).ConfigureAwait(false);
            await VerifyAsync(cache, key, value, cancellationToken).ConfigureAwait(false);
        }

        Console.WriteLine($"PASS: 96 old values read; 96 updates written and verified; batchMs={elapsed.ElapsedMilliseconds}");
    }

    private static async Task VerifyAsync(ICache<string> cache, string key, string expected, CancellationToken cancellationToken)
    {
        var result = await cache.GetValueAsync(key, cancellationToken).ConfigureAwait(false);
        if (!result.Found || !string.Equals(result.Value, expected, StringComparison.Ordinal))
            throw new InvalidOperationException($"Value mismatch for {key}: found={result.Found}, value={result.Value}");
    }
}
