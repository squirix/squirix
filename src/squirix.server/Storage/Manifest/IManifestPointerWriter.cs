using System;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Durable writer for the fixed-size SQMC <c>man-current</c> pointer.</summary>
internal interface IManifestPointerWriter : IDisposable
{
    /// <summary>Overwrites the in-place SQMC pointer payload.</summary>
    /// <param name="pointerBuffer">Exactly 12 encoded SQMC bytes.</param>
    void Write(ReadOnlySpan<byte> pointerBuffer);
}
