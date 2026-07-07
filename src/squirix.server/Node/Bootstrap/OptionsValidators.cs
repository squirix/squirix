using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Bootstrap;

internal static class OptionsValidators
{
    private static ValidateOptionsResult ToResult(List<string> failures) => failures.Count is 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(failures);

    [SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Constructed by the dependency injection container.")]
    internal sealed class StartupOptionsValidator<TOptions> : IHostedService
        where TOptions : class
    {
        private readonly IOptions<TOptions> _options;
        private readonly IValidateOptions<TOptions> _validator;

        public StartupOptionsValidator(IOptions<TOptions> options, IValidateOptions<TOptions> validator)
        {
            _options = options;
            _validator = validator;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            var result = _validator.Validate(Options.DefaultName, _options.Value);
            return result.Failed ? throw new OptionsValidationException(Options.DefaultName, typeof(TOptions), result.Failures) : Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    [SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Constructed by the dependency injection container.")]
    internal sealed class BackpressureOptionsValidator : IValidateOptions<AdmissionOptions>
    {
        public ValidateOptionsResult Validate(string? name, AdmissionOptions options)
        {
            try
            {
                options.Validate();
                return ValidateOptionsResult.Success;
            }
            catch (InvalidOperationException ex)
            {
                return ValidateOptionsResult.Fail(ex.Message);
            }
        }
    }

    [SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Constructed by the dependency injection container.")]
    internal sealed class ClusterConfigValidator : IValidateOptions<ClusterConfig>
    {
        public ValidateOptionsResult Validate(string? name, ClusterConfig options) =>
            ClusterTopologyValidator.TryValidate(options, out var failures) ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(failures);
    }

    [SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Constructed by the dependency injection container.")]
    internal sealed class IdempotencyOptionsValidator : IValidateOptions<IdempotencyOptions>
    {
        public ValidateOptionsResult Validate(string? name, IdempotencyOptions options)
        {
            try
            {
                options.Validate();
                return ValidateOptionsResult.Success;
            }
            catch (InvalidOperationException ex)
            {
                return ValidateOptionsResult.Fail(ex.Message);
            }
        }
    }

    [SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Constructed by the dependency injection container.")]
    internal sealed class JournalCompactionOptionsValidator : IValidateOptions<JournalCompactionOptions>
    {
        public ValidateOptionsResult Validate(string? name, JournalCompactionOptions options)
        {
            var failures = new List<string>();
            if (options.MinTailSegments < 0)
                failures.Add("journal compaction MinTailSegments cannot be negative.");
            if (options.MinTailBytes < 0)
                failures.Add("journal compaction MinTailBytes cannot be negative.");
            if (options.MinGap < TimeSpan.Zero)
                failures.Add("journal compaction MinGap cannot be negative.");

            return ToResult(failures);
        }
    }

    [SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Constructed by the dependency injection container.")]
    internal sealed class JournalMetricsExporterOptionsValidator : IValidateOptions<JournalMetricsExporterOptions>
    {
        public ValidateOptionsResult Validate(string? name, JournalMetricsExporterOptions options) => options.Interval > TimeSpan.Zero ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail("journal metrics exporter Interval must be greater than zero.");
    }

    [SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Constructed by the dependency injection container.")]
    internal sealed class MemoryPressureOptionsValidator : IValidateOptions<PressureOptions>
    {
        public ValidateOptionsResult Validate(string? name, PressureOptions options)
        {
            try
            {
                options.Validate();
                return ValidateOptionsResult.Success;
            }
            catch (InvalidOperationException ex)
            {
                return ValidateOptionsResult.Fail(ex.Message);
            }
        }
    }

    [SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Constructed by the dependency injection container.")]
    internal sealed class MtlsOptionsValidator : IValidateOptions<MtlsOptions>
    {
        private readonly ClusterConfig _cluster;

        public MtlsOptionsValidator(ClusterConfig cluster)
        {
            _cluster = cluster;
        }

        public ValidateOptionsResult Validate(string? name, MtlsOptions options)
        {
            try
            {
                var primaryListenPort = _cluster.Uri.IsAbsoluteUri ? _cluster.Uri.Port : default(int?);
                options.Validate(primaryListenPort, MtlsTopology.RequiresInterNodeMtls(_cluster));
                return ValidateOptionsResult.Success;
            }
            catch (InvalidOperationException ex)
            {
                return ValidateOptionsResult.Fail(ex.Message);
            }
        }
    }

    [SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Constructed by the dependency injection container.")]
    internal sealed class PersistenceOptionsValidator : IValidateOptions<PersistenceOptions>
    {
        public ValidateOptionsResult Validate(string? name, PersistenceOptions options)
        {
            var failures = new List<string>();
            if (string.IsNullOrWhiteSpace(options.DataDir))
                failures.Add("Persistence DataDir is required.");

            try
            {
                options.Validate();
            }
            catch (InvalidOperationException ex)
            {
                failures.Add(ex.Message);
            }

            return ToResult(failures);
        }
    }

    [SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Constructed by the dependency injection container.")]
    internal sealed class PrometheusEndpointOptionsValidator : IValidateOptions<PrometheusMetricsEndpointOptions>
    {
        public ValidateOptionsResult Validate(string? name, PrometheusMetricsEndpointOptions options) => options switch
        {
            { Enabled: false } => ValidateOptionsResult.Success,
            _ when string.IsNullOrWhiteSpace(options.Path) => ValidateOptionsResult.Fail("Prometheus metrics Path must be non-empty when the endpoint is enabled."),
            _ when !options.Path.StartsWith('/') => ValidateOptionsResult.Fail("Prometheus metrics Path must start with '/'."),
            _ => ValidateOptionsResult.Success,
        };
    }

    [SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Constructed by the dependency injection container.")]
    internal sealed class SnapshotTriggerOptionsValidator : IValidateOptions<TriggerOptions>
    {
        public ValidateOptionsResult Validate(string? name, TriggerOptions options)
        {
            var failures = new List<string>();
            if (options.SnapshotInterval <= TimeSpan.Zero)
                failures.Add("Snapshot SnapshotInterval must be greater than zero.");
            if (options.SnapshotEveryNOps < 0)
                failures.Add("Snapshot SnapshotEveryNOps cannot be negative.");
            if (options.SnapshotEveryNBytes < 0)
                failures.Add("Snapshot SnapshotEveryNBytes cannot be negative.");
            if (options.MinGapBetweenSnapshots < TimeSpan.Zero)
                failures.Add("Snapshot MinGapBetweenSnapshots cannot be negative.");
            if (options.JournalGrowthThrottleBytes < 0)
                failures.Add("Snapshot JournalGrowthThrottleBytes cannot be negative.");
            if (options.LatencySloMilliseconds < 0 || double.IsNaN(options.LatencySloMilliseconds) || double.IsInfinity(options.LatencySloMilliseconds))
                failures.Add("Snapshot LatencySloMilliseconds must be a finite non-negative value.");
            if (options.LatencyThrottleDuration < TimeSpan.Zero)
                failures.Add("Snapshot LatencyThrottleDuration cannot be negative.");

            return ToResult(failures);
        }
    }
}
