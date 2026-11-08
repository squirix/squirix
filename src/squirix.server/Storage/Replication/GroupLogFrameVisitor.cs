using System;

namespace Squirix.Server.Storage.Replication;

/// <summary>Consumes a validated log frame synchronously without taking ownership of its payload.</summary>
/// <param name="logIndex">The frame log index.</param>
/// <param name="term">The frame term.</param>
/// <param name="payload">Canonical bytes valid only until this callback returns; retained data must be copied.</param>
/// <returns>Whether to visit the next frame.</returns>
internal delegate bool GroupLogFrameVisitor(ulong logIndex, ulong term, ReadOnlyMemory<byte> payload);
