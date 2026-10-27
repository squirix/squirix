using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Errors;

/// <summary>Covers FailedPrecondition detail classification helpers.</summary>
[Immutable]
public sealed class ServerOpContractClassifierTests : ServerUnitTestBase
{
    /// <summary>Recognizes operation-id reuse mismatch details.</summary>
    [Test]
    public async Task DetectsOperationIdReuseMismatchDetail()
    {
        _ = await Assert.That(ServerOpContractClassifier.IsOperationIdReuseMismatchDetail(ServerOpIdMismatchException.StableDetail)).IsTrue();
        _ = await Assert.That(ServerOpContractClassifier.IsOperationIdReuseMismatchDetail(null)).IsFalse();
        _ = await Assert.That(ServerOpContractClassifier.IsOperationIdReuseMismatchDetail("other")).IsFalse();
    }

    /// <summary>Exposes insert-version FailedPrecondition details as invalid-operation messages.</summary>
    [Test]
    public async Task InsertVersionDetailMapsToInvalidOp()
    {
        const string detail = "Version must be greater than current (current=1, provided=0)";
        _ = await Assert.That(ServerOpContractClassifier.TryGetFailedPreconditionMessage(detail, out var message)).IsTrue();
        _ = await Assert.That(message).IsEqualTo(detail);

        _ = await Assert.That(ServerOpContractClassifier.TryGetFailedPreconditionMessage(ServerOpIdMismatchException.StableDetail, out var reuse)).IsFalse();
        _ = await Assert.That(reuse).IsNull();
        _ = await Assert.That(ServerOpContractClassifier.TryGetFailedPreconditionMessage(null, out _)).IsFalse();
    }
}
