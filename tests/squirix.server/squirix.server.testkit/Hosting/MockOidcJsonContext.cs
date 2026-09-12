using System.Text.Json.Serialization;
using Microsoft.IdentityModel.Tokens;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Source-generated JSON metadata for <see cref="MockOidcAuthority" /> payloads (AOT-safe, no reflection).</summary>
[JsonSourceGenerationOptions(RespectNullableAnnotations = false, RespectRequiredConstructorParameters = false)]
[JsonSerializable(typeof(MockOidcAuthority.OidcDiscoveryDocument))]
[JsonSerializable(typeof(JsonWebKeySet))]
internal sealed partial class MockOidcJsonContext : JsonSerializerContext;
