using System;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Utils;

/// <summary>Shared helpers for reading optional sections from <c language="csharp">Squirix.settings.json</c>.</summary>
internal static class SettingsJson
{
    private static readonly JsonDocumentOptions DocumentOptions = new() { AllowTrailingCommas = true, CommentHandling = JsonCommentHandling.Skip };

    internal static string? FindSettingsPath() => FileEx.FindFile(["Squirix.settings.json", "squirix.settings.json"]);

    internal static async Task<T> WithSquirixRootAsync<TState, T>(string settingsFilePath, TState state, Func<JsonElement, TState, T> action, CancellationToken cancellationToken)
    {
        var validatedPath = FilePathValidator.ResolveValidatedFilePath(settingsFilePath);
        var bytes = await File.ReadAllBytesAsync(validatedPath, cancellationToken).ConfigureAwait(false);
        using var doc = JsonDocument.Parse(bytes, DocumentOptions);
        var root = doc.RootElement;
        if (root.TryGetProperty("Squirix", out var squirix))
            root = squirix;

        return action(root, state);
    }
}
