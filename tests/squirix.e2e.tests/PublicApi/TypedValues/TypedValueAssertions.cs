using System;
using System.Linq;
using Xunit;

namespace Squirix.E2ETests.PublicApi.TypedValues;

internal static class TypedValueAssertions
{
    public static void AssertCartEquals(TypedMutableCart expected, TypedMutableCart actual)
    {
        Assert.Equal(expected.Id, actual.Id);
        Assert.Equal(expected.Total, actual.Total);
        Assert.Equal(expected.UpdatedAt, actual.UpdatedAt);
        Assert.Equal(expected.CouponCode, actual.CouponCode);
        Assert.Equal(expected.Items.Count, actual.Items.Count);

        foreach (var (expectedItem, actualItem) in expected.Items.Zip(actual.Items))
        {
            Assert.Equal(expectedItem.Sku, actualItem.Sku);
            Assert.Equal(expectedItem.Quantity, actualItem.Quantity);
            Assert.Equal(expectedItem.Price, actualItem.Price);
        }
    }

    public static void AssertProfileEquals(TypedCustomerProfile expected, TypedCustomerProfile actual)
    {
        Assert.Equal(expected.Id, actual.Id);
        Assert.Equal(expected.DisplayName, actual.DisplayName);
        Assert.Equal(expected.Email, actual.Email);
        AssertAddressEquals(expected.Address, actual.Address);
        Assert.Equal(expected.Roles, actual.Roles);
        Assert.Equal(expected.Metadata.Count, actual.Metadata.Count);
        foreach (var item in expected.Metadata)
        {
            Assert.True(
                actual.Metadata.TryGetValue(item.Key, out var value) && string.Equals(value, item.Value, StringComparison.OrdinalIgnoreCase),
                $"missing metadata {item.Key}");
        }

        Assert.Equal(expected.CreatedAt, actual.CreatedAt);
        Assert.Equal(expected.Status, actual.Status);
    }

    private static void AssertAddressEquals(TypedCustomerAddress expected, TypedCustomerAddress actual)
    {
        Assert.Equal(expected.City, actual.City);
        Assert.Equal(expected.Street, actual.Street);
        Assert.Equal(expected.PostalCode, actual.PostalCode);
        Assert.Equal(expected.Country, actual.Country);
        Assert.Equal(expected.Metadata.Count, actual.Metadata.Count);
        foreach (var item in expected.Metadata)
        {
            Assert.True(
                actual.Metadata.TryGetValue(item.Key, out var value) && string.Equals(value, item.Value, StringComparison.OrdinalIgnoreCase),
                $"missing address metadata {item.Key}");
        }
    }
}
