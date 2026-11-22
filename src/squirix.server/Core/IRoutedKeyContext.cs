namespace Squirix.Server.Core;

/// <summary>Carries key validation and the ring group for an inbound request.</summary>
internal interface IRoutedKeyContext
{
    void RecordRoute(string cacheName, string key, ServerKeyValidationError? error, string? groupId);

    bool TryGet(string? key, out ServerKeyValidationError? error);

    string? FindGroup(string cacheName, string key);
}
