using Grpc.Core;
using Squirix.Errors;
using Xunit;

namespace Squirix.UnitTests.Errors;

/// <summary>
/// Characterization tests for <see cref="CacheOperationContractClassifier" /> stable transport classification.
/// </summary>
public sealed class CacheOperationContractClassifierTests
{
    /// <summary>
    /// Insert explicit-version precondition detail classifies as insert-version contract.
    /// </summary>
    [Fact]
    public void ClassifyFailedPreconditionDetailMapsInsertVersionMessage()
    {
        var detail = CacheOperationContract.InsertVersionMustExceedCurrentMessage(3, 1);
        Assert.Equal(CacheOperationFailedPreconditionKind.InsertVersionMustExceedCurrent, CacheOperationContractClassifier.ClassifyFailedPreconditionDetail(detail));
    }

    /// <summary>
    /// Unrelated FailedPrecondition text does not match stable contracts.
    /// </summary>
    [Fact]
    public void ClassifyFailedPreconditionDetailReturnsNoneForGenericDetail() => Assert.Equal(
        CacheOperationFailedPreconditionKind.None,
        CacheOperationContractClassifier.ClassifyFailedPreconditionDetail("unrelated precondition"));

    /// <summary>
    /// Counter overflow uses InvalidArgument with the stable overflow detail constant.
    /// </summary>
    [Fact]
    public void IsCounterOverflowRpcFaultMatchesInvalidArgumentWithStableDetail()
    {
        Assert.True(CacheOperationContractClassifier.IsCounterOverflowRpcFault(StatusCode.InvalidArgument, CacheOperationContract.CounterOverflowDetail));
        Assert.False(CacheOperationContractClassifier.IsCounterOverflowRpcFault(StatusCode.InvalidArgument, "other"));
        Assert.False(CacheOperationContractClassifier.IsCounterOverflowRpcFault(StatusCode.FailedPrecondition, CacheOperationContract.CounterOverflowDetail));
    }

    /// <summary>
    /// TryGet returns the original detail string for stable contracts.
    /// </summary>
    [Fact]
    public void TryGetFailedPreconditionInvalidOperationMessageReturnsDetailForInsertVersion()
    {
        var detail = CacheOperationContract.InsertVersionMustExceedCurrentMessage(9, 2);
        Assert.True(CacheOperationContractClassifier.TryGetFailedPreconditionInvalidOperationMessage(detail, out var message));
        Assert.Equal(detail, message);
    }

    /// <summary>
    /// TryGet mirrors Classify for invalid-operation message paths.
    /// </summary>
    [Fact]
    public void TryGetFailedPreconditionInvalidOperationMessageReturnsFalseForNone() =>
        Assert.False(CacheOperationContractClassifier.TryGetFailedPreconditionInvalidOperationMessage("x", out _));
}
