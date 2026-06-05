using System;
using Squirix.Server.Node.Hosting;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Hosting;

/// <summary>
/// Verifies hosted cache DI registration order against <see cref="CachePipelineDescriptor" />.
/// </summary>
public sealed class CachePipelineOrderTests
{
    /// <summary>
    /// Ensures domain error mapping wraps deadline (deadline registers before domain error mapping in inner-to-outer order).
    /// </summary>
    [Fact]
    public void CachePipelineKeepsDomainErrorMappingOutsideDeadline()
    {
        var source = LoadRegistrationSource();
        var deadline = CachePipelineRegistrationOrderAssertions.IndexOfRequired(source, CachePipelineLayer.Deadline);
        var domain = CachePipelineRegistrationOrderAssertions.IndexOfRequired(source, CachePipelineLayer.DomainErrorMapping);
        Assert.True(deadline < domain, "Deadline must register before DomainErrorMapping so domain mapping wraps deadline at runtime.");
    }

    /// <summary>
    /// Ensures Journal logging wraps the local physical branch (client or optional accounting) before ownership routing.
    /// </summary>
    [Fact]
    public void CachePipelineKeepsJournalBeforeLocalMutation()
    {
        var source = LoadRegistrationSource();
        var client = CachePipelineRegistrationOrderAssertions.IndexOfRequired(source, CachePipelineLayer.ClientCache);
        var journal = CachePipelineRegistrationOrderAssertions.IndexOfRequired(source, CachePipelineLayer.JournalLogging);
        Assert.True(client < journal, "ClientCache must be registered before JournalLogging so the Journal factory wraps the local branch.");

        Assert.Contains("GetRequiredService<ClientCache<object?>>()", source, StringComparison.Ordinal);
        Assert.Contains("GetRequiredService<JournalLoggingCacheDecorator<object?>>()", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// Ensures memory admission sits outside the owner-local Journal branch (admission registration after journal in inner-to-outer source order).
    /// </summary>
    [Fact]
    public void CachePipelineKeepsMemoryAdmissionBeforeJournal()
    {
        var source = LoadRegistrationSource();
        var journal = CachePipelineRegistrationOrderAssertions.IndexOfRequired(source, CachePipelineLayer.JournalLogging);
        var admission = CachePipelineRegistrationOrderAssertions.IndexOfRequired(source, CachePipelineLayer.MemoryAdmission);
        Assert.True(journal < admission, "JournalLogging must register before MemoryAdmission so admission wraps the routed branch outside journal.");
    }

    /// <summary>
    /// Ensures metrics decorate after admission so admission helper reads are not counted as primary logical operations.
    /// </summary>
    [Fact]
    public void CachePipelineKeepsMetricsOutsideAdmission()
    {
        var source = LoadRegistrationSource();
        var admission = CachePipelineRegistrationOrderAssertions.IndexOfRequired(source, CachePipelineLayer.MemoryAdmission);
        var metrics = CachePipelineRegistrationOrderAssertions.IndexOfRequired(source, CachePipelineLayer.Metrics);
        Assert.True(admission < metrics, "MemoryAdmission must register before Metrics.");
    }

    /// <summary>
    /// Ensures tracing remains the outermost decorator before the <c>ILogicalNamespacedCache&lt;T&gt;</c> registration.
    /// </summary>
    [Fact]
    public void CachePipelineKeepsTracingOutermost()
    {
        var source = LoadRegistrationSource();
        CachePipelineRegistrationOrderAssertions.AssertTracingPrecedesLogicalAggregate(source);

        var tracing = CachePipelineRegistrationOrderAssertions.IndexOfRequired(source, CachePipelineLayer.Tracing);

        foreach (var layer in CachePipelineDescriptor.AddSingletonRegistrationInnerToOuter)
        {
            if (layer == CachePipelineLayer.Tracing)
            {
                continue;
            }

            var prior = CachePipelineRegistrationOrderAssertions.IndexOfRequired(source, layer);
            Assert.True(prior < tracing, $"{layer} registration must appear before Tracing (inner-to-outer registration order).");
        }
    }

    /// <summary>
    /// Ensures <see cref="SquirixCachePipelineRegistration" /> singleton markers appear in the order defined by
    /// <see cref="CachePipelineDescriptor.AddSingletonRegistrationInnerToOuter" /> and exposes the logical boundary through tracing.
    /// </summary>
    [Fact]
    public void CachePipelineOrderDescriptorMatchesRegistration()
    {
        var source = LoadRegistrationSource();
        CachePipelineRegistrationOrderAssertions.AssertSingletonOrderMatchesDescriptor(source);
        CachePipelineRegistrationOrderAssertions.AssertTracingPrecedesLogicalAggregate(source);
    }

    private static string LoadRegistrationSource() => CachePipelineRegistrationOrderAssertions.LoadPipelineRegistrationSource();
}
