using System;
using System.Text;

namespace Squirix.Server.Storage.Manifest.Binary;

/// <summary>In-memory manifest index and payload cached after the first load or publish.</summary>
internal sealed class BinaryManifestState
{
    public bool IsInitialized { get; private set; }

    public ManifestState Current { get; private set; } = new();

    public int CurrentIndex { get; private set; }

    public ReadOnlyMemory<byte> SnapshotPathUtf8 { get; private set; } = ReadOnlyMemory<byte>.Empty;

    public void Set(ManifestState manifest, int index)
    {
        Current = manifest;
        CurrentIndex = index;
        SnapshotPathUtf8 = manifest.LastSnapshot?.Path is { Length: > 0 } path
            ? Encoding.UTF8.GetBytes(path)
            : ReadOnlyMemory<byte>.Empty;
        IsInitialized = true;
    }
}
