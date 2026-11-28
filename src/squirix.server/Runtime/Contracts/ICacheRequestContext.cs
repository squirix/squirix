using Squirix.Server.Core;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>Context required while routing and executing an inbound cache request.</summary>
internal interface ICacheRequestContext : IRoutedKeyContext
{
    bool Forwarded { get; }

    bool ReportUnknownMutationOutcome { get; }

    string? AdmissionClientId { get; }

    object? LogScope => null;

    bool IsAdmitted(object gate);
}
