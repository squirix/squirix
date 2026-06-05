using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace Squirix.Server.Serialization;

internal static class DurabilityJson
{
    public static JsonDocumentOptions StrictDocumentOptions { get; } = new()
    {
        AllowDuplicateProperties = false,
        AllowTrailingCommas = false,
        CommentHandling = JsonCommentHandling.Disallow,
    };

    public static JsonSerializerOptions StrictSerializerOptions { get; } = CreateStrictSerializerOptions();

    private static JsonSerializerOptions CreateStrictSerializerOptions()
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web)
        {
            AllowDuplicateProperties = false,
            AllowTrailingCommas = false,
            PropertyNameCaseInsensitive = true,
            ReadCommentHandling = JsonCommentHandling.Disallow,
            UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
        };
        options.Converters.Add(new JsonStringEnumConverter());
        options.TypeInfoResolverChain.Insert(0, SquirixJsonSerializerContext.Default);
        options.TypeInfoResolverChain.Add(new DefaultJsonTypeInfoResolver());
        return options;
    }
}
