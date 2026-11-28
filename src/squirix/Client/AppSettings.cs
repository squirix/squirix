using System;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Client;

internal static class AppSettings
{
    private const string FileName = "appsettings.json";
    private static readonly JsonDocumentOptions DocumentOptions = new() { AllowTrailingCommas = true, CommentHandling = JsonCommentHandling.Skip };

    internal static async Task<SquirixClientOptions> LoadAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var path = Path.Join(Directory.GetCurrentDirectory(), FileName);
        if (!File.Exists(path))
            path = Path.Join(AppContext.BaseDirectory, FileName);

        if (!File.Exists(path))
            throw new InvalidOperationException($"Configure Squirix:Client:Endpoints in {FileName} before connecting.");

        var bytes = await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
        using var document = JsonDocument.Parse(bytes, DocumentOptions);
        return Read(document.RootElement);
    }

    internal static SquirixClientOptions Read(JsonElement root)
    {
        if (!TryGetProperty(root, "Squirix", out var squirix)
            || !TryGetProperty(squirix, "Client", out var client)
            || !TryGetProperty(client, "Endpoints", out var endpoints)
            || endpoints.ValueKind != JsonValueKind.Array
            || endpoints.GetArrayLength() == 0)
            throw new InvalidOperationException($"{FileName} must define Squirix:Client:Endpoints as a non-empty array.");

        var options = new SquirixClientOptions();
        foreach (var value in endpoints.EnumerateArray())
        {
            if (value.ValueKind != JsonValueKind.String || !Uri.TryCreate(value.GetString(), UriKind.Absolute, out var endpoint))
                throw new InvalidOperationException($"Squirix:Client:Endpoints in {FileName} must contain absolute endpoint URIs.");

            options.Endpoints.Add(endpoint);
        }

        return options;
    }

    private static bool TryGetProperty(JsonElement element, string name, out JsonElement value)
    {
        if (element.ValueKind == JsonValueKind.Object)
        {
            foreach (var property in element.EnumerateObject())
            {
                if (!string.Equals(property.Name, name, StringComparison.OrdinalIgnoreCase))
                    continue;

                value = property.Value;
                return true;
            }
        }

        value = default;
        return false;
    }
}
