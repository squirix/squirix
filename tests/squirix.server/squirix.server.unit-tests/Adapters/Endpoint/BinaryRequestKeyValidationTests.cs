using System;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Core;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Endpoint;

/// <summary>Exercises how routed keys share a validation result within a single binary request.</summary>
public sealed class BinaryRequestKeyValidationTests : ServerUnitTestBase
{
    /// <summary>Both success and failure remain bound to the original request and key.</summary>
    /// <param name="key">The key received at the wire boundary.</param>
    /// <param name="valid">Whether the key meets the server rules.</param>
    [Test]
    [Arguments("valid", true)]
    [Arguments("", false)]
    public async Task RoutedResultStaysWithRequest(string key, bool valid)
    {
        var requests = new BinaryRequestAccessor { Current = new BinaryRequestContext { Path = "/cache/Get" } };
        var accepted = CacheKeyValidator.TryValidate(key, out var routeError);
        requests.RecordRoute("cache", key, accepted ? null : routeError, accepted ? "group" : null);
        var found = requests.TryGet(key, out var savedError);

        _ = await Assert.That(accepted).IsEqualTo(valid);
        _ = await Assert.That(found).IsTrue();
        ServerKeyValidationError? expectedError = valid ? null : ServerKeyValidationError.Required;
        _ = await Assert.That(savedError).IsEqualTo(expectedError);
        _ = await Assert.That(requests.FindGroup(new string("cache".AsSpan()), key)).IsEqualTo(valid ? "group" : null);
        _ = await Assert.That(requests.FindGroup("other-cache", key)).IsNull();
        _ = await Assert.That(requests.FindGroup("cache", "other-key")).IsNull();

        const string otherKey = "other\u0001";
        _ = await Assert.That(requests.TryGet(otherKey, out _)).IsFalse();
        var localAccepted = CacheKeyValidator.TryValidate(otherKey, requests, out var localError);
        _ = await Assert.That(localAccepted).IsFalse();
        _ = await Assert.That(localError).IsEqualTo(ServerKeyValidationError.ControlCharacters);

        requests.Current = new BinaryRequestContext { Path = "/cache/Get" };
        _ = await Assert.That(requests.TryGet(key, out _)).IsFalse();
        _ = await Assert.That(requests.FindGroup("cache", key)).IsNull();
    }
}
