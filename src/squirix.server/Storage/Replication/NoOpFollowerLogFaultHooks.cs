using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>Fault hooks that do nothing, used when none are supplied.</summary>
[Immutable]
internal sealed class NoOpFollowerLogFaultHooks : IFollowerLogFaultHooks
{
    private NoOpFollowerLogFaultHooks()
    {
    }

    /// <summary>Gets the shared instance.</summary>
    internal static NoOpFollowerLogFaultHooks Instance { get; } = new();

    /// <inheritdoc />
    public void OnBeforeMemoryApply()
    {
    }

    /// <inheritdoc />
    public void OnCommitAdvanced()
    {
    }

    /// <inheritdoc />
    public void OnFlushed()
    {
    }

    /// <inheritdoc />
    public void OnFrameWritten()
    {
    }

    /// <inheritdoc />
    public void OnMetaPublishing()
    {
    }

    /// <inheritdoc />
    public void OnSnapshotPublishing()
    {
    }
}
