using System;
using System.Collections.Generic;

namespace Squirix.E2ETests.PublicApi.TypedValues;

internal static class TypedValueFactory
{
    private static readonly DateTimeOffset BaseInstant = new(2026, 6, 6, 8, 15, 30, TimeSpan.Zero);

    public static TypedMutableCart CreateCart(string id) => new()
    {
        Id = id,
        Items =
        [
            new TypedCartItem { Sku = "SKU-001", Quantity = 2, Price = 12.50m },
            new TypedCartItem { Sku = "SKU-002", Quantity = 1, Price = 7.25m },
        ],
        Total = 32.25m,
        UpdatedAt = BaseInstant.AddHours(1),
        CouponCode = "SAVE10",
    };

    public static TypedCustomerProfile CreateProfile(string id) => new(
        id,
        $"Customer {id}",
        $"{id}@example.test",
        CreateAddress("Singapore", "Marina Boulevard 10", "018983", "SG"),
        ["customer", "buyer"],
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["source"] = "e2e",
            ["tier"] = "gold",
        },
        BaseInstant,
        TypedCustomerStatus.Active);

    public static TypedCustomerProfile CreateProfileWithEmptyCollections(string id) => CreateProfile(id) with
    {
        Roles = [],
        Metadata = new Dictionary<string, string>(StringComparer.Ordinal),
    };

    public static TypedCustomerProfile CreateProfileWithNullEmail(string id) => CreateProfile(id) with
    {
        Email = null,
    };

    public static TypedCustomerProfile CreateProfileWithUnicodeText(string id) => CreateProfile(id) with
    {
        DisplayName = "Customer 東京",
        Address = CreateAddress("東京", "千代田 1-1", "100-0001", "JP"),
        Metadata = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["note"] = "Привет",
            ["emoji-text"] = "cafe",
        },
    };

    public static TypedMutableCart CreateUpdatedCart(string id) => new()
    {
        Id = id,
        Items =
        [
            new TypedCartItem { Sku = "SKU-003", Quantity = 3, Price = 4.75m },
        ],
        Total = 14.25m,
        UpdatedAt = BaseInstant.AddHours(2),
        CouponCode = null,
    };

    public static TypedCustomerProfile CreateUpdatedProfile(string id) => new(
        id,
        $"Updated Customer {id}",
        null,
        CreateAddress("Berlin", "Unter den Linden 77", "10117", "DE"),
        ["buyer", "reviewer"],
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["source"] = "e2e-updated",
            ["tier"] = "platinum",
            ["flag"] = "typed",
        },
        BaseInstant.AddMinutes(30),
        TypedCustomerStatus.Suspended);

    private static TypedCustomerAddress CreateAddress(string city, string street, string postalCode, string country) => new(
        city,
        street,
        postalCode,
        country,
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["kind"] = "billing",
            ["verified"] = "true",
        });
}
