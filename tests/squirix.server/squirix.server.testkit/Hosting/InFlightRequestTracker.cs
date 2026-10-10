using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Counts the requests a node serves, so a stop can report how many were in flight when it began and when the last one finished.</summary>
internal sealed class InFlightRequestTracker
{
    private readonly Lock _gate = new();
    private int _active;
    private int _activeAtStop;
    private long _lastFinished;
    private bool _stopping;
    private long _stopStarted;

    /// <summary>Registers the tracker and the middleware that feeds it.</summary>
    /// <param name="services">The service collection of the node.</param>
    internal static void AddTo(IServiceCollection services)
    {
        services.TryAddSingleton<InFlightRequestTracker>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IStartupFilter, TrackingStartupFilter>());
    }

    /// <summary>Marks the start of the stop and remembers how many requests are being served.</summary>
    /// <param name="timestamp">The <see cref="Stopwatch" /> timestamp of the start of the stop.</param>
    internal void BeginStop(long timestamp)
    {
        lock (_gate)
        {
            _stopStarted = timestamp;
            _activeAtStop = _active;
            _stopping = true;
        }
    }

    /// <summary>Reads what the tracker saw since the stop began.</summary>
    /// <returns>The requests in flight when the stop began, and the time until the last request finished.</returns>
    internal (int InFlightAtStop, double? LastFinishedMs) Snapshot()
    {
        lock (_gate)
        {
            double? finished = _lastFinished == 0 ? null : Stopwatch.GetElapsedTime(_stopStarted, _lastFinished).TotalMilliseconds;
            return (_activeAtStop, finished);
        }
    }

    private void Enter()
    {
        lock (_gate)
            _active++;
    }

    private void Exit()
    {
        lock (_gate)
        {
            _active--;
            if (_stopping)
                _lastFinished = Stopwatch.GetTimestamp();
        }
    }

    private sealed class TrackingStartupFilter : IStartupFilter
    {
        private readonly InFlightRequestTracker _tracker;

        public TrackingStartupFilter(InFlightRequestTracker tracker)
        {
            _tracker = tracker;
        }

        public Action<IApplicationBuilder> Configure(Action<IApplicationBuilder> next) => app =>
        {
            _ = app.Use(TrackAsync);
            next(app);
        };

        private async Task TrackAsync(HttpContext context, RequestDelegate next)
        {
            _tracker.Enter();
            try
            {
                await next(context).ConfigureAwait(false);
            }
            finally
            {
                _tracker.Exit();
            }
        }
    }
}
