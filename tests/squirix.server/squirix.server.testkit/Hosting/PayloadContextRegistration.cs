using System.Text.Json.Serialization;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Registers consumer payload metadata with the server serializer chain for tests.</summary>
public static class PayloadContextRegistration
{
    /// <summary>Adds a source-generation context to the server metadata resolution chain.</summary>
    /// <param name="context">Context covering the payload types under test.</param>
    public static void RegisterServerPayloadContext(JsonSerializerContext context) =>
        Core.SerializerMetadata.RegisterContext(context);
}
