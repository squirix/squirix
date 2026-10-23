using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Core;

/// <summary>A side-effect-free read of a stored entry for admission: the live entry, or none, and whether an entry past its deadline still waits for its expiry.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
/// <remarks>
/// One read and one clock reading decide all three states, so a clock that steps between two reads cannot turn an expired entry live again.
/// The default value reads the key as absent.
/// </remarks>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct NodeCacheEntryPeek<T>
{
    /// <summary>Initializes a new instance of the <see cref="NodeCacheEntryPeek{T}" /> struct.</summary>
    /// <param name="stored">The stored entry, or <see langword="null" /> when the key is absent.</param>
    /// <param name="expired">Whether <paramref name="stored" /> is past its deadline; ignored when it is <see langword="null" />.</param>
    internal NodeCacheEntryPeek(NodeCacheEntry<T>? stored, bool expired)
    {
        Live = expired ? null : stored;
        ExpiryPending = expired && stored != null;
    }

    /// <summary>Gets the live entry, or <see langword="null" /> when the key is absent or its entry is past its deadline.</summary>
    internal NodeCacheEntry<T>? Live { get; }

    /// <summary>Gets a value indicating whether the key holds an entry past its deadline that no committed record has removed yet.</summary>
    internal bool ExpiryPending { get; }
}
