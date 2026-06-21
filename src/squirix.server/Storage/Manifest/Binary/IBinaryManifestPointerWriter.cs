using System;

namespace Squirix.Server.Storage.Manifest.Binary;

/// <summary>Durable writer for the fixed-size SQMC <c>man-current</c> pointer.</summary>
internal interface IBinaryManifestPointerWriter : IDisposable
{
    /// <summary>Gets the Linux file descriptor for io_uring batch durability, or <c>-1</c> when unavailable.</summary>
    int UnixFileDescriptor { get; }

    /// <summary>Overwrites the in-place SQMC pointer payload.</summary>
    /// <param name="pointerBuffer">Exactly 12 encoded SQMC bytes.</param>
    void Write(ReadOnlySpan<byte> pointerBuffer);
}
