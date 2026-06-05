using Grpc.Core;

// ReSharper disable once CheckNamespace
namespace Squirix.Transport.Grpc.Mappers;

/// <summary>
/// Stable transport signals for stale-owner routing failures on internal cluster gRPC calls.
/// </summary>
internal static class GrpcStaleOwnerMarkers
{
    private const string ErrorCodeMetadataKey = "squirix-error-code";
    private const string StaleOwnerErrorCodeValue = "stale-owner";

    internal static Metadata CreateStaleOwnerTrailers() => new() { { ErrorCodeMetadataKey, StaleOwnerErrorCodeValue } };
}
