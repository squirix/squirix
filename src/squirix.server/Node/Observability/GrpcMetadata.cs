using Grpc.Core;

namespace Squirix.Server.Node.Observability;

/// <summary>Shared helpers for copying gRPC metadata between bags.</summary>
internal static class GrpcMetadata
{
    /// <summary>Copies all entries from <paramref name="source" /> into <paramref name="target" />.</summary>
    /// <param name="target">Metadata bag receiving the entries.</param>
    /// <param name="source">Metadata bag providing the entries.</param>
    internal static void CopyInto(Metadata target, Metadata source)
    {
        for (var i = 0; i < source.Count; i++)
        {
            var entry = source[i];
            if (entry.IsBinary)
                target.Add(entry.Key, entry.ValueBytes);
            else
                target.Add(entry.Key, entry.Value);
        }
    }
}
