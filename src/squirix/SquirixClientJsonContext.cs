using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Squirix;

/// <summary>Source-generated JSON metadata for client cache primitives (AOT-safe, no reflection).</summary>
[JsonSourceGenerationOptions(RespectNullableAnnotations = false, RespectRequiredConstructorParameters = false)]
[JsonSerializable(typeof(JsonElement))]
[JsonSerializable(typeof(string))]
[JsonSerializable(typeof(bool))]
[JsonSerializable(typeof(int))]
[JsonSerializable(typeof(long))]
[JsonSerializable(typeof(double))]
[JsonSerializable(typeof(decimal))]
[JsonSerializable(typeof(Dictionary<string, int>))]
internal sealed partial class SquirixClientJsonContext : JsonSerializerContext;
