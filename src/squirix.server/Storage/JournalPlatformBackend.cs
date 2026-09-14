namespace Squirix.Server.Storage;

/// <summary>Selects the platform segment writer.</summary>
public enum JournalPlatformBackend
{
    /// <summary>Resolves to <c language="csharp">RandomAccess</c>.</summary>
    Auto = 0,

    /// <summary><see cref="System.IO.RandomAccess" /> batched writes.</summary>
    RandomAccess = 1,
}
