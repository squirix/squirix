namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Exposes the current node identity for admin and health endpoints.
/// </summary>
internal interface INodeEndpointIdentity
{
    /// <summary>
    /// Gets the current node id.
    /// </summary>
    string NodeId { get; }

    /// <summary>
    /// Gets the current node listen URL.
    /// </summary>
    string Url { get; }
}
