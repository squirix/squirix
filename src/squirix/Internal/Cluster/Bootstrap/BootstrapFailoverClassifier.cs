using System;
using System.IO;
using System.Net.Http;
using Grpc.Core;

namespace Squirix.Internal.Cluster.Bootstrap;

internal static class BootstrapFailoverClassifier
{
    internal static bool IsFailoverEligible(Exception exception) => exception switch
    {
        RpcException rpc => rpc.StatusCode is StatusCode.Unavailable or StatusCode.DeadlineExceeded or StatusCode.Internal or StatusCode.ResourceExhausted,
        HttpRequestException => true,
        IOException => true,
        _ => false,
    };
}
