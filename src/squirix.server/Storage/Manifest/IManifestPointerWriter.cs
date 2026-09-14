using System;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Durable writer for the fixed-size SQMC <c language="csharp">man-current</c> pointer.</summary>
internal interface IManifestPointerWriter
{
    /// <summary>Overwrites the in-place SQMC pointer payload.</summary>
    /// <param name="buffer">Exactly 12 encoded SQMC bytes.</param>
    void Write(ReadOnlySpan<byte> buffer);
}
