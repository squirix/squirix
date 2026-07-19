using System;
using System.Collections.Generic;

namespace Squirix.E2ETests.Fixtures.TypedValues;

internal sealed record TypedCustomerProfile(
    string Id,
    string DisplayName,
    string? Email,
    TypedCustomerAddress Address,
    IReadOnlyList<string> Roles,
    Dictionary<string, string> Metadata,
    DateTimeOffset CreatedAt,
    TypedCustomerStatus Status);
