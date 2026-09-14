using System;
using System.Collections.Generic;
using Squirix.Attributes;

namespace Squirix.E2ETests.Fixtures.TypedValues;

[Immutable]
internal sealed record TypedCustomerProfile(
    string Id,
    string DisplayName,
    string? Email,
    TypedCustomerAddress Address,
    IReadOnlyList<string> Roles,
    Dictionary<string, string> Metadata,
    DateTimeOffset CreatedAt,
    TypedCustomerStatus Status);
