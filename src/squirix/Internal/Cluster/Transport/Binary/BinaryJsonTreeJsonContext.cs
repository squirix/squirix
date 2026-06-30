using System.Text.Json;
using System.Text.Json.Serialization;

namespace Squirix.Internal.Cluster.Transport.Binary;

/// <summary>Source-generated JSON metadata for <see cref="BinaryJsonTreeCodec" /> wire decode.</summary>
[JsonSerializable(typeof(JsonElement))]
internal sealed partial class BinaryJsonTreeJsonContext : JsonSerializerContext;
