namespace Squirix.Server.Node.Backpressure;

/// <summary>Reserved admission identities for forwarded and in-process calls.</summary>
internal static class ClientIds
{
    /// <summary>Forwarded owner calls use only free node capacity and never enter a client queue.</summary>
    internal const string InternalOwner = "internal";

    /// <summary>Calls without a transport identity share this admission bucket.</summary>
    internal const string Runtime = "runtime";
}
