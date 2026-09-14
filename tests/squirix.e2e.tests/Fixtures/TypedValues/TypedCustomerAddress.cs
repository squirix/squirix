using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Squirix.Attributes;

namespace Squirix.E2ETests.Fixtures.TypedValues;

[SuppressMessage("ReSharper", "NotAccessedPositionalProperty.Global", Justification = "Positional properties are exercised through typed-value serialization round trips.")]
[Immutable]
internal sealed record TypedCustomerAddress(string City, string Street, string PostalCode, string Country, Dictionary<string, string> Metadata);
