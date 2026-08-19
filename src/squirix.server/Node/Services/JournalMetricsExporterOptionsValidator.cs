using System;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Options;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Services;

[SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Constructed by the dependency injection container.")]
[Immutable]
internal sealed class JournalMetricsExporterOptionsValidator : IValidateOptions<JournalMetricsExporterOptions>
{
    public ValidateOptionsResult Validate(string? name, JournalMetricsExporterOptions options) => options.Interval > TimeSpan.Zero ? ValidateOptionsResult.Success
        : ValidateOptionsResult.Fail("journal metrics exporter Interval must be greater than zero.");
}
