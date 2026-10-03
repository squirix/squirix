using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Backpressure;

/// <summary>Gives every cache operation one shared backpressure client id, for hosts that set no per-client limit.</summary>
[Immutable]
internal sealed class SharedClientIdResolver : IBackpressureClientIdResolver
{
    private SharedClientIdResolver()
    {
    }

    internal static SharedClientIdResolver Instance { get; } = new();

    /// <inheritdoc />
    public string Resolve() => HttpContextClientIdResolver.MissingHttpContextClientId;
}
