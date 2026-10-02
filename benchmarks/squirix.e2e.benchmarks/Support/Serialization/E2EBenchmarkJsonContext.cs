using System.Runtime.CompilerServices;
using System.Text.Json.Serialization;
using Squirix.E2EBenchmarks.Fixtures;
using Squirix.Server.TestKit.Hosting;

namespace Squirix.E2EBenchmarks.Support.Serialization;

/// <summary>Source-generated JSON metadata for the structured benchmark values (AOT-safe, no reflection).</summary>
/// <remarks>The client receives it through <see cref="Squirix.Client.SquirixClientOptions.JsonSerializerContexts" />; the in-process
/// server chain gets it at assembly load.</remarks>
[JsonSourceGenerationOptions(RespectNullableAnnotations = false, RespectRequiredConstructorParameters = false)]
[JsonSerializable(typeof(BenchmarkUserProfile))]
[JsonSerializable(typeof(BenchmarkOrder))]
internal sealed partial class E2EBenchmarkJsonContext : JsonSerializerContext
{
    /// <summary>Registers the benchmark value metadata with the server serializer chain.</summary>
    [ModuleInitializer]
    internal static void RegisterServerMetadata() => PayloadContextRegistration.RegisterServerPayloadContext(Default);
}
