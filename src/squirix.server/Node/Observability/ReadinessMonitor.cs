using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Composition;
using Squirix.Server.Logging;

namespace Squirix.Server.Node.Observability;

/// <summary>Runs the node's readiness checks independently and reports their worst status.</summary>
internal sealed class ReadinessMonitor
{
    private readonly ReadinessCheck[] _checks;
    private readonly IServiceProvider _services;
    private readonly IServerLogger<ReadinessMonitor> _logger;

    internal ReadinessMonitor(IEnumerable<ReadinessCheck> checks, IServiceProvider services, IServerLogger<ReadinessMonitor> logger)
    {
        _checks = [.. checks];
        _services = services;
        _logger = logger;
    }

    internal async Task<ReadinessStatus> GetStatusAsync(CancellationToken cancellationToken)
    {
        var status = ReadinessStatus.Healthy;
        var results = await CheckAsync(cancellationToken).ConfigureAwait(false);
        foreach (var (_, result) in results)
        {
            if (result.Status < status)
                status = result.Status;
        }

        return status;
    }

    internal Task<(string Name, ReadinessResult Result)[]> CheckAsync(CancellationToken cancellationToken)
    {
        var tasks = new Task<(string Name, ReadinessResult Result)>[_checks.Length];
        for (var i = 0; i < _checks.Length; i++)
            tasks[i] = RunAsync(_checks[i], cancellationToken);

        return Task.WhenAll(tasks);
    }

    private static void LogCheckFailed(IServerLogger logger, string checkName, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.LogCheckFailedEvent.Write(logger, ServerLogLevel.Error, exception, ValueTuple.Create(checkName));
    }

    private static void LogNotReady(IServerLogger logger, ServerLogLevel level, string checkName, ReadinessStatus status, string description)
    {
        if (logger.IsEnabled(level))
            Events.LogNotReadyEvent.Write(logger, level, null, (checkName, status, description));
    }

    private async Task<(string Name, ReadinessResult Result)> RunAsync(ReadinessCheck registration, CancellationToken cancellationToken)
    {
        // Start checks independently, even when a check completes synchronously.
        await Task.CompletedTask.ConfigureAwait(ConfigureAwaitOptions.ForceYielding);
        cancellationToken.ThrowIfCancellationRequested();
        var scope = _services.Find<INodeRequestScopes>()?.Open() ?? new NodeRequestScope(_services, null);
        await using (scope.ConfigureAwait(false))
        {
            var check = registration.Create(scope.ServiceProvider);
            try
            {
                var result = await check.CheckHealthAsync(cancellationToken).ConfigureAwait(false);
                if (result.Status != ReadinessStatus.Healthy)
                    LogNotReady(_logger, result.Status == ReadinessStatus.Unhealthy ? ServerLogLevel.Error : ServerLogLevel.Warning, registration.Name, result.Status, result.Description);
                return (registration.Name, result);
            }
            catch (Exception exception) when (exception is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
            {
                LogCheckFailed(_logger, registration.Name, exception);
                var description = exception is OperationCanceledException ? "A timeout occurred while running check." : exception.Message;
                return (registration.Name, ReadinessResult.Unhealthy(description));
            }
        }
    }

    private static class Events
    {
        internal static readonly LogEvent LogCheckFailedEvent = new(3038, nameof(ReadinessMonitor.LogCheckFailed), "Readiness check {CheckName} threw an exception");

        internal static readonly LogEvent LogNotReadyEvent = new(3039, nameof(ReadinessMonitor.LogNotReady), "Readiness check {CheckName} reported {Status}: {Description}");
    }
}
