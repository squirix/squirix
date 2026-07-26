namespace Squirix.Server.Node.Backpressure;

/// <summary>Resolves the backpressure client id for the current cache operation.</summary>
internal interface IBackpressureClientIdResolver
{
    /// <summary>Returns a stable, non-empty client id used for per-client admission limits.</summary>
    /// <returns>The client id for this operation.</returns>
    string Resolve();
}
