using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.Node.Observability;

/// <summary>Runs the node's readiness checks independently and reports their worst status.</summary>
internal sealed partial class ReadinessMonitor
{
    private readonly ReadinessCheck[] _checks;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<ReadinessMonitor> _logger;

    internal ReadinessMonitor(IEnumerable<ReadinessCheck> checks, IServiceScopeFactory scopeFactory, ILogger<ReadinessMonitor> logger)
    {
        _checks = [.. checks];
        _scopeFactory = scopeFactory;
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

    [LoggerMessage(EventId = 3038, Level = LogLevel.Error, Message = "Readiness check {CheckName} threw an exception")]
    private static partial void LogCheckFailed(ILogger logger, string checkName, Exception exception);

    [LoggerMessage(EventId = 3039, Message = "Readiness check {CheckName} reported {Status}: {Description}")]
    private static partial void LogNotReady(ILogger logger, LogLevel level, string checkName, ReadinessStatus status, string description);

    private async Task<(string Name, ReadinessResult Result)> RunAsync(ReadinessCheck registration, CancellationToken cancellationToken)
    {
        // Start checks independently, even when a check completes synchronously.
        await Task.CompletedTask.ConfigureAwait(ConfigureAwaitOptions.ForceYielding);
        cancellationToken.ThrowIfCancellationRequested();
        var scope = _scopeFactory.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            var check = registration.Create(scope.ServiceProvider);
            try
            {
                var result = await check.CheckHealthAsync(cancellationToken).ConfigureAwait(false);
                if (result.Status != ReadinessStatus.Healthy)
                    LogNotReady(_logger, result.Status == ReadinessStatus.Unhealthy ? LogLevel.Error : LogLevel.Warning, registration.Name, result.Status, result.Description);
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
}
