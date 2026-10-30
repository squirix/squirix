using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.MemoryPressure;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Tests admission decisions independently of metrics, cache implementations, and product exceptions.</summary>
[Immutable]
public sealed class AdmissionPolicyTests
{
    /// <summary>Known non-growing operations do not query memory usage.</summary>
    /// <param name="growth">The estimated net growth of the operation.</param>
    [Test]
    [Arguments(0L)]
    [Arguments(-1L)]
    [Arguments(long.MinValue)]
    public async Task NonGrowingWriteDoesNotReadUsage(long growth)
    {
        var accounting = new IMemoryUsageAccountingCreateExpectations();
        var evaluator = new StateEvaluator(new PressureOptions { MaxEstimatedCacheBytes = 1000 });

        var decision = AdmissionPolicy.Evaluate(evaluator, accounting.Instance(), growth, false);

        _ = await Assert.That(decision).IsEqualTo(AdmissionRejection.None);
    }

    /// <summary>Projected usage saturates rather than overflowing into an admitted negative value.</summary>
    [Test]
    public async Task ProjectedGrowthSaturatesAtLongMax()
    {
        var accounting = new IMemoryUsageAccountingCreateExpectations();
        _ = accounting.Setups.ReadEstimatedBytes().ReturnValue(1024);
        var evaluator = new StateEvaluator(new PressureOptions { MaxEstimatedCacheBytes = long.MaxValue, CriticalPressureThresholdPercent = 100 });

        var decision = AdmissionPolicy.Evaluate(evaluator, accounting.Instance(), long.MaxValue, false);

        _ = await Assert.That(decision).IsEqualTo(AdmissionRejection.EstimatedLimit);
    }
}
