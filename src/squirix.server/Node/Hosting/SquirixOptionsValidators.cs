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
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Hosting;

internal static class SquirixOptionsValidators
{
    private static ValidateOptionsResult ToResult(List<string> failures) => failures.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(failures);

    [SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Constructed by the dependency injection container.")]
    internal sealed class BackpressureOptionsValidator : IValidateOptions<BackpressureOptions>
    {
        public ValidateOptionsResult Validate(string? name, BackpressureOptions options)
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

        public MtlsOptionsValidator(ClusterConfig cluster) => _cluster = cluster;

        public ValidateOptionsResult Validate(string? name, MtlsOptions options)
        {
            try
            {
                var primaryListenPort = Uri.TryCreate(_cluster.Url, UriKind.Absolute, out var uri) ? uri.Port : (int?)null;
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
    internal sealed class ClusterConfigValidator : IValidateOptions<ClusterConfig>
    {
        public ValidateOptionsResult Validate(string? name, ClusterConfig options) =>
            ClusterTopologyValidator.TryValidate(options, out var failures) ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(failures);
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
        public ValidateOptionsResult Validate(string? name, JournalMetricsExporterOptions options) => options.Interval > TimeSpan.Zero
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail("journal metrics exporter Interval must be greater than zero.");
    }

    [SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Constructed by the dependency injection container.")]
    internal sealed class MemoryPressureOptionsValidator : IValidateOptions<MemoryPressureOptions>
    {
        public ValidateOptionsResult Validate(string? name, MemoryPressureOptions options)
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
    internal sealed class PersistenceOptionsValidator : IValidateOptions<PersistenceOptions>
    {
        public ValidateOptionsResult Validate(string? name, PersistenceOptions options)
        {
            var failures = new List<string>();
            if (string.IsNullOrWhiteSpace(options.DataDir))
                failures.Add("Persistence DataDir is required.");
            if (options.JournalMaxSegmentMb <= 0)
                failures.Add("Persistence JournalMaxSegmentMb must be greater than zero.");
            if (options.FlushIntervalMs <= 0)
                failures.Add("Persistence FlushIntervalMs must be greater than zero.");
            if (options.SnapshotIntervalSec <= 0)
                failures.Add("Persistence SnapshotIntervalSec must be greater than zero.");
            if (options.ManifestRetentionCount <= 0)
                failures.Add("Persistence ManifestRetentionCount must be greater than zero.");
            if (options.SnapshotRetentionCount <= 0)
                failures.Add("Persistence SnapshotRetentionCount must be greater than zero.");
            if (options.JournalGroupCommitMaxWaitMs < 0)
                failures.Add("Persistence JournalGroupCommitMaxWaitMs cannot be negative.");
            if (options.JournalGroupCommitMaxBatch <= 0)
                failures.Add("Persistence JournalGroupCommitMaxBatch must be greater than zero.");

            return ToResult(failures);
        }
    }

    [SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Constructed by the dependency injection container.")]
    internal sealed class PrometheusMetricsEndpointOptionsValidator : IValidateOptions<PrometheusMetricsEndpointOptions>
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
    internal sealed class SnapshotTriggerOptionsValidator : IValidateOptions<SnapshotTriggerOptions>
    {
        public ValidateOptionsResult Validate(string? name, SnapshotTriggerOptions options)
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

    [SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Constructed by the dependency injection container.")]
    internal sealed class StartupOptionsValidator<TOptions> : IHostedService
        where TOptions : class
    {
        private readonly IOptions<TOptions> _options;
        private readonly IEnumerable<IValidateOptions<TOptions>> _validators;

        public StartupOptionsValidator(IOptions<TOptions> options, IEnumerable<IValidateOptions<TOptions>> validators)
        {
            _options = options;
            _validators = validators;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            var failures = new List<string>();
            foreach (var validator in _validators)
            {
                var result = validator.Validate(Options.DefaultName, _options.Value);
                if (result.Failed)
                    failures.AddRange(result.Failures);
            }

            return failures.Count > 0 ? throw new OptionsValidationException(Options.DefaultName, typeof(TOptions), failures) : Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
