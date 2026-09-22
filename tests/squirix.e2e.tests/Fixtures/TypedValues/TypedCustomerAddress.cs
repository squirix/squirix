using System.Collections.Generic;
using Squirix.Attributes;

namespace Squirix.E2ETests.Fixtures.TypedValues;

[Immutable]
internal sealed record TypedCustomerAddress(string City, string Street, string PostalCode, string Country, Dictionary<string, string> Metadata);
