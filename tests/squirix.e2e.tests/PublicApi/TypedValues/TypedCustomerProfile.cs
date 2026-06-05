using System;
using System.Collections.Generic;

namespace Squirix.E2ETests.PublicApi.TypedValues;

internal sealed record TypedCustomerProfile(
    string Id,
    string DisplayName,
    string? Email,
    TypedCustomerAddress Address,
    string[] Roles,
    Dictionary<string, string> Metadata,
    DateTimeOffset CreatedAt,
    TypedCustomerStatus Status);
