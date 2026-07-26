using Squirix.Server.Errors;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Errors;

/// <summary>Covers FailedPrecondition detail classification helpers.</summary>
public sealed class ServerOpContractClassifierTests : ServerUnitTestBase
{
    /// <summary>Recognizes operation-id reuse mismatch details.</summary>
    [Fact]
    public void DetectsOperationIdReuseMismatchDetail()
    {
        Assert.True(ServerOpContractClassifier.IsOperationIdReuseMismatchDetail(ServerOpIdMismatchException.StableDetail));
        Assert.False(ServerOpContractClassifier.IsOperationIdReuseMismatchDetail(null));
        Assert.False(ServerOpContractClassifier.IsOperationIdReuseMismatchDetail("other"));
    }

    /// <summary>Exposes insert-version FailedPrecondition details as invalid-operation messages.</summary>
    [Fact]
    public void MapsInsertVersionDetailToInvalidOperation()
    {
        const string detail = "Version must be greater than current (current=1, provided=0)";
        Assert.True(ServerOpContractClassifier.TryGetFailedPreconditionInvalidOperationMessage(detail, out var message));
        Assert.Equal(detail, message);

        Assert.False(ServerOpContractClassifier.TryGetFailedPreconditionInvalidOperationMessage(ServerOpIdMismatchException.StableDetail, out var reuse));
        Assert.Null(reuse);
        Assert.False(ServerOpContractClassifier.TryGetFailedPreconditionInvalidOperationMessage(null, out _));
    }
}
