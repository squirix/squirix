using System;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client;

namespace Squirix.Server.Cluster.Transport;

internal static class GrpcChannelConnectWarmup
{
    public static async ValueTask ConnectWithRetryAsync(
        GrpcChannel channel,
        string endpointName,
        BootstrapConnectOptions options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(channel);
        ArgumentException.ThrowIfNullOrWhiteSpace(endpointName);

        var deadlineUtc = DateTime.UtcNow + options.OverallDeadline;
        Exception? lastFailure = null;
        var attempt = 0;

        while (DateTime.UtcNow < deadlineUtc)
        {
            cancellationToken.ThrowIfCancellationRequested();
            attempt++;

            var remaining = deadlineUtc - DateTime.UtcNow;
            if (remaining <= TimeSpan.Zero)
                break;

            var attemptTimeout = remaining < options.PerAttemptTimeout ? remaining : options.PerAttemptTimeout;

            using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            attemptCts.CancelAfter(attemptTimeout);

            try
            {
                await channel.ConnectAsync(attemptCts.Token).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                lastFailure = new InvalidOperationException(
                    $"Failed to connect to endpoint '{endpointName}' within {options.PerAttemptTimeout.TotalMilliseconds}ms.");
            }
            catch (HttpRequestException ex)
            {
                lastFailure = ex;
            }
            catch (IOException ex)
            {
                lastFailure = ex;
            }
            catch (RpcException ex)
            {
                lastFailure = ex;
            }

            remaining = deadlineUtc - DateTime.UtcNow;
            if (remaining <= TimeSpan.Zero)
                break;

            var backoff = BackoffWithJitter(attempt, options);
            if (backoff > remaining)
                backoff = remaining;

            await Task.Delay(backoff, cancellationToken).ConfigureAwait(false);
        }

        throw lastFailure ?? new InvalidOperationException(
            $"Failed to connect to endpoint '{endpointName}' within {options.OverallDeadline.TotalSeconds}s.");
    }

    private static TimeSpan BackoffWithJitter(int attempt, BootstrapConnectOptions options)
    {
        var pow = Math.Min(attempt - 1, 6);
        var cappedMs = Math.Min(options.MaxBackoff.TotalMilliseconds, options.BaseBackoff.TotalMilliseconds * Math.Pow(2, pow));
        var jitterFactor = 0.5 + (Random.Shared.NextDouble() * 0.5);
        var finalMs = Math.Max(cappedMs * jitterFactor, Math.Min(50.0, cappedMs));
        return TimeSpan.FromMilliseconds(finalMs);
    }
}
