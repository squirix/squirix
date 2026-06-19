namespace Squirix.Server.Storage.Journaling.PipelinedWal;

/// <summary>Selects the platform segment writer used by <see cref="JournalBackend.PipelinedWal"/>.</summary>
public enum WalPlatformBackend
{
    /// <summary>Linux with io_uring available → <c>Uring</c>; otherwise <c>RandomAccess</c>.</summary>
    Auto,

    /// <summary><see cref="System.IO.RandomAccess"/> batched writes.</summary>
    RandomAccess,

    /// <summary>Linux io_uring segment writer.</summary>
    Uring,
}
