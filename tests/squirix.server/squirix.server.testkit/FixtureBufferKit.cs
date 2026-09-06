using System;

namespace Squirix.Server.TestKit;

/// <summary>Exact-size owned byte buffer helpers for testkit fixtures.</summary>
internal static class FixtureBufferKit
{
    /// <summary>Copies a span into an exact-size owned byte buffer retained beyond the encode call.</summary>
    /// <param name="source">Source bytes to copy.</param>
    /// <returns>An owned byte array containing the source bytes.</returns>
    internal static byte[] CopyToOwned(ReadOnlySpan<byte> source)
    {
        // ZA0302: owned test fixture escape; the buffer outlives the borrowed encode scratch.
        var owned = new byte[source.Length];
        source.CopyTo(owned);
        return owned;
    }
}
