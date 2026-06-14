using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace Squirix.E2ETests.PublicApi.TypedValues;

[SuppressMessage("ReSharper", "NotAccessedPositionalProperty.Global", Justification = "Positional properties are exercised through typed-value serialization round trips.")]
internal sealed record TypedCustomerAddress(
    string City,
    string Street,
    string PostalCode,
    string Country,
    Dictionary<string, string> Metadata);
