using System;
using System.Text.Json;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Errors;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Shared assertions for REST/gRPC error contract tests.</summary>
internal static class ErrorContractTestKit
{
    /// <summary>Asserts canonical REST error JSON fields.</summary>
    /// <param name="payload">Parsed error response body.</param>
    /// <param name="status">Observed HTTP status code.</param>
    /// <param name="expectedStatus">Expected HTTP status code.</param>
    /// <param name="expectedError">Expected stable error name.</param>
    /// <param name="expectedPublicCode">Expected public error token.</param>
    /// <param name="expectedDetail">Expected bounded detail text.</param>
    internal static async Task AssertErrorJsonPayloadAsync(
        JsonDocument payload,
        int status,
        int expectedStatus,
        string expectedError,
        string expectedPublicCode,
        string expectedDetail)
    {
        ArgumentNullException.ThrowIfNull(payload);

        _ = await Assert.That(status).IsEqualTo(expectedStatus);
        _ = await Assert.That(payload.RootElement.GetProperty("error").GetString()).IsEqualTo(expectedError);
        _ = await Assert.That(payload.RootElement.GetProperty("code").GetString()).IsEqualTo(expectedPublicCode);
        _ = await Assert.That(payload.RootElement.GetProperty("detail").GetString()).IsEqualTo(expectedDetail);
    }

    /// <summary>Asserts a resource-exhausted gRPC mapping for a structured squirix error contract.</summary>
    /// <param name="contract">Structured error from <see cref="ServerOpContract" />.</param>
    /// <param name="expectedCode">Expected stable squirix error code.</param>
    /// <param name="expectedPublicCode">Expected public error token.</param>
    /// <param name="expectedDetail">Expected bounded detail text.</param>
    /// <param name="createDirectRpc">Factory for the direct exception RPC projection.</param>
    internal static async Task AssertResourceExhaustedGrpcMappingAsync(
        SquirixException contract,
        SquirixErrorCode expectedCode,
        string expectedPublicCode,
        string expectedDetail,
        Func<RpcException> createDirectRpc)
    {
        ArgumentNullException.ThrowIfNull(contract);
        ArgumentNullException.ThrowIfNull(createDirectRpc);

        _ = await Assert.That(contract.Code).IsEqualTo(expectedCode);
        _ = await Assert.That(SquirixErrorMapper.ToPublicCode(contract.Code)).IsEqualTo(expectedPublicCode);

        var rpc = contract.ToRpcException();
        _ = await Assert.That(rpc.StatusCode).IsEqualTo(StatusCode.ResourceExhausted);
        _ = await Assert.That(rpc.Status.Detail).IsEqualTo(expectedDetail);

        var direct = createDirectRpc();
        _ = await Assert.That(direct.StatusCode).IsEqualTo(StatusCode.ResourceExhausted);
        _ = await Assert.That(direct.Status.Detail).IsEqualTo(expectedDetail);
    }
}
