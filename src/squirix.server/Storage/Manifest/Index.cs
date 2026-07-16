using System;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Manifest;

/// <summary>In-memory manifest index and payload cached after the first load or publish.</summary>
internal sealed class Index
{
    internal State Current { get; private set; } = new();

    internal int CurrentIndex { get; private set; }

    internal bool IsInitialized { get; private set; }

    internal ReadOnlyMemory<byte> SnapshotPathUtf8 { get; private set; } = ReadOnlyMemory<byte>.Empty;

    internal void Set(State manifest, int index)
    {
        Current = manifest;
        CurrentIndex = index;
        SnapshotPathUtf8 = manifest.LastSnapshot?.Path is { Length: > 0 } path ? EncodeUtf8(path) : ReadOnlyMemory<byte>.Empty;
        IsInitialized = true;
    }

    private static byte[] EncodeUtf8(string text) => BufferEx.Utf8ToOwned(text);
}
