using System;
using System.Text.Json;
using Grpc.Core;
using Squirix.Server.Errors;
using Xunit;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Shared assertions for REST/gRPC error contract tests.</summary>
internal static class ErrorContractTestKit
{
    /// <summary>Asserts a resource-exhausted gRPC mapping for a structured squirix error contract.</summary>
    /// <param name="contract">Structured error from <see cref="ServerOpContract" />.</param>
    /// <param name="expectedCode">Expected stable squirix error code.</param>
    /// <param name="expectedPublicCode">Expected public error token.</param>
    /// <param name="expectedDetail">Expected bounded detail text.</param>
    /// <param name="createDirectRpc">Factory for the direct exception RPC projection.</param>
    internal static void AssertResourceExhaustedGrpcMapping(
        SquirixException contract,
        SquirixErrorCode expectedCode,
        string expectedPublicCode,
        string expectedDetail,
        Func<RpcException> createDirectRpc)
    {
        ArgumentNullException.ThrowIfNull(contract);
        ArgumentNullException.ThrowIfNull(createDirectRpc);

        Assert.Equal(expectedCode, contract.Code);
        Assert.Equal(expectedPublicCode, SquirixErrorMapper.ToPublicCode(contract.Code));

        var rpc = contract.ToRpcException();
        Assert.Equal(StatusCode.ResourceExhausted, rpc.StatusCode);
        Assert.Equal(expectedDetail, rpc.Status.Detail);

        var direct = createDirectRpc();
        Assert.Equal(StatusCode.ResourceExhausted, direct.StatusCode);
        Assert.Equal(expectedDetail, direct.Status.Detail);
    }

    /// <summary>Asserts canonical REST error JSON fields.</summary>
    /// <param name="payload">Parsed error response body.</param>
    /// <param name="status">Observed HTTP status code.</param>
    /// <param name="expectedStatus">Expected HTTP status code.</param>
    /// <param name="expectedError">Expected stable error name.</param>
    /// <param name="expectedPublicCode">Expected public error token.</param>
    /// <param name="expectedDetail">Expected bounded detail text.</param>
    internal static void AssertErrorJsonPayload(
        JsonDocument payload,
        int status,
        int expectedStatus,
        string expectedError,
        string expectedPublicCode,
        string expectedDetail)
    {
        ArgumentNullException.ThrowIfNull(payload);

        Assert.Equal(expectedStatus, status);
        Assert.Equal(expectedError, payload.RootElement.GetProperty("error").GetString());
        Assert.Equal(expectedPublicCode, payload.RootElement.GetProperty("code").GetString());
        Assert.Equal(expectedDetail, payload.RootElement.GetProperty("detail").GetString());
    }
}
