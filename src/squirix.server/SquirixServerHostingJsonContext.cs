using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server;

/// <summary>Source-generated JSON metadata for public server hosting configuration.</summary>
[JsonSourceGenerationOptions(
    JsonSerializerDefaults.Web,
    PropertyNameCaseInsensitive = true,
    ReadCommentHandling = JsonCommentHandling.Skip,
    AllowTrailingCommas = true,
    RespectNullableAnnotations = false,
    RespectRequiredConstructorParameters = false)]
[JsonSerializable(typeof(SquirixServerOptions))]
[JsonSerializable(typeof(SquirixServerPeerOptions))]
[JsonSerializable(typeof(SquirixServerBackpressureOptions))]
[JsonSerializable(typeof(SquirixServerJournalOptions))]
[JsonSerializable(typeof(TriggerOptions))]
[JsonSerializable(typeof(PressureSettings))]
[JsonSerializable(typeof(PrometheusMetricsSettings))]
[JsonSerializable(typeof(JsonElement))]
[JsonSerializable(typeof(string))]
[JsonSerializable(typeof(bool))]
[JsonSerializable(typeof(int))]
[JsonSerializable(typeof(long))]
[JsonSerializable(typeof(double))]
[JsonSerializable(typeof(decimal))]
[JsonSerializable(typeof(Dictionary<string, int>))]
internal sealed partial class SquirixServerHostingJsonContext : JsonSerializerContext;
