using System;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace Squirix.E2ETests.Fixtures.TypedValues;

internal static class TypedValueAssertions
{
    internal static async Task AssertCartEquals(TypedMutableCart expected, TypedMutableCart actual)
    {
        _ = await Assert.That(actual.Id).IsEqualTo(expected.Id);
        _ = await Assert.That(actual.Total).IsEqualTo(expected.Total);
        _ = await Assert.That(actual.UpdatedAt).IsEqualTo(expected.UpdatedAt);
        _ = await Assert.That(actual.CouponCode).IsEqualTo(expected.CouponCode);
        _ = await Assert.That(actual.Items.Count).IsEqualTo(expected.Items.Count);

        for (var i = 0; i < expected.Items.Count; i++)
        {
            var expectedItem = expected.Items[i];
            var actualItem = actual.Items[i];
            _ = await Assert.That(actualItem.Sku).IsEqualTo(expectedItem.Sku);
            _ = await Assert.That(actualItem.Quantity).IsEqualTo(expectedItem.Quantity);
            _ = await Assert.That(actualItem.Price).IsEqualTo(expectedItem.Price);
        }
    }

    internal static async Task AssertProfileEquals(TypedCustomerProfile expected, TypedCustomerProfile actual)
    {
        _ = await Assert.That(actual.Id).IsEqualTo(expected.Id);
        _ = await Assert.That(actual.DisplayName).IsEqualTo(expected.DisplayName);
        _ = await Assert.That(actual.Email).IsEqualTo(expected.Email);
        await AssertAddressEquals(expected.Address, actual.Address);
        await SequenceAssert.Equal(expected.Roles, actual.Roles, StringComparer.Ordinal);
        _ = await Assert.That(actual.Metadata.Count).IsEqualTo(expected.Metadata.Count);
        foreach (var item in expected.Metadata)
            _ = await Assert.That(actual.Metadata.TryGetValue(item.Key, out var value) && string.Equals(value, item.Value, StringComparison.OrdinalIgnoreCase)).IsTrue();

        _ = await Assert.That(actual.CreatedAt).IsEqualTo(expected.CreatedAt);
        _ = await Assert.That(actual.Status).IsEqualTo(expected.Status);
    }

    private static async Task AssertAddressEquals(TypedCustomerAddress expected, TypedCustomerAddress actual)
    {
        _ = await Assert.That(actual.City).IsEqualTo(expected.City);
        _ = await Assert.That(actual.Street).IsEqualTo(expected.Street);
        _ = await Assert.That(actual.PostalCode).IsEqualTo(expected.PostalCode);
        _ = await Assert.That(actual.Country).IsEqualTo(expected.Country);
        _ = await Assert.That(actual.Metadata.Count).IsEqualTo(expected.Metadata.Count);
        foreach (var item in expected.Metadata)
            _ = await Assert.That(actual.Metadata.TryGetValue(item.Key, out var value) && string.Equals(value, item.Value, StringComparison.OrdinalIgnoreCase)).IsTrue();
    }
}
