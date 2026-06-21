namespace Squirix.Server.Storage.Manifest.Binary;

/// <summary>In-memory manifest index and payload cached after the first load or publish.</summary>
internal sealed class BinaryManifestState
{
    public bool IsInitialized { get; private set; }

    public ManifestState Current { get; private set; } = new();

    public int CurrentIndex { get; private set; }

    public void Set(ManifestState manifest, int index)
    {
        Current = manifest;
        CurrentIndex = index;
        IsInitialized = true;
    }
}
