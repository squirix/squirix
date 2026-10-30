using System.Threading;

namespace Squirix.Server.Storage.Journaling.Abstractions;

/// <summary>Flows the journal-facing operation context with the current asynchronous execution.</summary>
internal static class JournalMutationContext
{
    private static readonly AsyncLocal<IJournalMutationContext?> Active = new();

    /// <summary>Gets or sets the operation context; its owner restores the previous context when the scope ends.</summary>
    internal static IJournalMutationContext? Current
    {
        get => Active.Value;
        set => Active.Value = value;
    }
}
