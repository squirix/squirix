using System.Text.Json.Serialization;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Source-generated JSON metadata for <see cref="MockOidcAuthority" /> payloads (AOT-safe, no reflection).</summary>
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase, RespectNullableAnnotations = false, RespectRequiredConstructorParameters = false)]
[JsonSerializable(typeof(OidcDiscoveryDocument))]
[JsonSerializable(typeof(MockOidcAuthority.JwksDocument))]
internal sealed partial class MockOidcJsonContext : JsonSerializerContext;
