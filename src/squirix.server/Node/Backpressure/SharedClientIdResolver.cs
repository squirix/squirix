using Squirix.Server.Attributes;
using Squirix.Server.Runtime.Invocation;

namespace Squirix.Server.Node.Backpressure;

/// <summary>
/// Gives every cache operation one shared backpressure client id, for hosts that set no per-client limit; internal owner-routed calls keep
/// their own id so the gate can admit them only to a free slot.
/// </summary>
[Immutable]
internal sealed class SharedClientIdResolver : IBackpressureClientIdResolver
{
    private SharedClientIdResolver()
    {
    }

    internal static SharedClientIdResolver Instance { get; } = new();

    /// <inheritdoc />
    public string Resolve() => RemoteInvocationContext.IsInternalOwnerInvocation
        ? HttpContextClientIdResolver.InternalOwnerClientId
        : HttpContextClientIdResolver.MissingHttpContextClientId;
}
