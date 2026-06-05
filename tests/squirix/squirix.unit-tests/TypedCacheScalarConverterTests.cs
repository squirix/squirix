using System;
using System.Collections.Generic;
using System.Text.Json;
using Squirix.Runtime;
using Xunit;

namespace Squirix.UnitTests;

/// <summary>
/// Unit tests for <see cref="TypedCacheScalarConverter" /> strict conversion rules.
/// </summary>
public sealed class TypedCacheScalarConverterTests
{
    private enum ByteBacked : byte
    {
        A = 1,
    }

    private enum IntBacked
    {
        Z = 99,
    }

    /// <summary>
    /// Gets integral scalar conversion cases that should be accepted without parsing.
    /// </summary>
    public static IEnumerable<TheoryDataRow<object, Type, object>> IntegralScalarConversionCases
    {
        get
        {
            yield return new TheoryDataRow<object, Type, object>((sbyte)-8, typeof(short), (short)-8);
            yield return new TheoryDataRow<object, Type, object>((byte)8, typeof(ushort), (ushort)8);
            yield return new TheoryDataRow<object, Type, object>((short)-16, typeof(int), -16);
            yield return new TheoryDataRow<object, Type, object>((ushort)16, typeof(uint), 16U);
            yield return new TheoryDataRow<object, Type, object>(32, typeof(long), 32L);
            yield return new TheoryDataRow<object, Type, object>(32U, typeof(ulong), 32UL);
            yield return new TheoryDataRow<object, Type, object>(64L, typeof(decimal), 64m);
            yield return new TheoryDataRow<object, Type, object>(64UL, typeof(double), 64d);
        }
    }

    /// <summary>
    /// Verifies arbitrary CLR objects are not coerced to scalars.
    /// </summary>
    [Fact]
    public void ArbitraryObjectRejectsIntTarget() => Assert.False(TypedCacheScalarConverter.TryConvertObject(new object(), typeof(int), out _));

    /// <summary>
    /// Verifies <see cref="bool" /> targets only accept <see cref="bool" /> instances.
    /// </summary>
    [Fact]
    public void BoolTargetAcceptsBoolAndRejectsInt()
    {
        Assert.True(TypedCacheScalarConverter.TryConvertObject(true, typeof(bool), out var b));
        Assert.True(Assert.IsType<bool>(b));
        Assert.False(TypedCacheScalarConverter.TryConvertObject(1, typeof(bool), out _));
    }

    /// <summary>
    /// Verifies <see cref="byte" />-backed enums reject numeric values outside the underlying range.
    /// </summary>
    [Fact]
    public void ByteEnumTargetRejectsOutOfRangeUnderlyingValue() => Assert.False(TypedCacheScalarConverter.TryConvertObject(300, typeof(ByteBacked), out _));

    /// <summary>
    /// Verifies <see cref="char" /> targets only accept <see cref="char" /> instances.
    /// </summary>
    [Fact]
    public void CharTargetAcceptsCharAndRejectsInt()
    {
        Assert.True(TypedCacheScalarConverter.TryConvertObject('z', typeof(char), out var c));
        Assert.Equal('z', Assert.IsType<char>(c));
        Assert.False(TypedCacheScalarConverter.TryConvertObject(122, typeof(char), out _));
    }

    /// <summary>
    /// Verifies <see cref="DateTimeOffset" /> identity conversion succeeds.
    /// </summary>
    [Fact]
    public void DateTimeOffsetTargetAcceptsDateTimeOffsetOnly()
    {
        var dto = new DateTimeOffset(2026, 5, 5, 12, 0, 0, TimeSpan.Zero);
        Assert.True(TypedCacheScalarConverter.TryConvertObject(dto, typeof(DateTimeOffset), out var o));
        Assert.Equal(dto, o);
        Assert.False(TypedCacheScalarConverter.TryConvertObject(dto.UtcDateTime, typeof(DateTimeOffset), out _));
    }

    /// <summary>
    /// Verifies <see cref="DateTime" /> identity conversion succeeds; numeric coercion is not supported.
    /// </summary>
    [Fact]
    public void DateTimeTargetAcceptsDateTimeOnly()
    {
        var dt = new DateTime(2026, 5, 5, 12, 0, 0, DateTimeKind.Utc);
        Assert.True(TypedCacheScalarConverter.TryConvertObject(dt, typeof(DateTime), out var o));
        Assert.Equal(dt, o);
        Assert.False(TypedCacheScalarConverter.TryConvertObject(0L, typeof(DateTime), out _));
    }

    /// <summary>
    /// Verifies <see cref="decimal" /> targets accept integer widening.
    /// </summary>
    [Fact]
    public void DecimalTargetAcceptsInt()
    {
        Assert.True(TypedCacheScalarConverter.TryConvertObject(9, typeof(decimal), out var o));
        Assert.Equal(9m, o);
    }

    /// <summary>
    /// Verifies strict decimal conversion rejects non-finite and overflowing floating-point values.
    /// </summary>
    [Fact]
    public void DecimalTargetRejectsNonFiniteAndOverflowingFloatingPointValues()
    {
        Assert.False(TypedCacheScalarConverter.TryConvertObject(double.NaN, typeof(decimal), out _));
        Assert.False(TypedCacheScalarConverter.TryConvertObject(double.MaxValue, typeof(decimal), out _));
        Assert.False(TypedCacheScalarConverter.TryConvertObject(float.PositiveInfinity, typeof(decimal), out _));
    }

    /// <summary>
    /// Verifies scalar default values convert consistently through supported target types.
    /// </summary>
    [Fact]
    public void DefaultScalarValuesConvertConsistently()
    {
        Assert.True(TypedCacheScalarConverter.TryConvertObject(0, typeof(long), out var longValue));
        Assert.Equal(0L, longValue);
        Assert.True(TypedCacheScalarConverter.TryConvertObject(false, typeof(bool?), out var boolValue));
        Assert.False((bool)boolValue);
        Assert.True(TypedCacheScalarConverter.TryConvertObject('\0', typeof(char), out var charValue));
        Assert.Equal('\0', charValue);
    }

    /// <summary>
    /// Verifies <see cref="double" /> target accepts integer widening.
    /// </summary>
    [Fact]
    public void DoubleTargetAcceptsInt()
    {
        Assert.True(TypedCacheScalarConverter.TryConvertObject(7, typeof(double), out var o));
        Assert.Equal(7.0d, o);
    }

    /// <summary>
    /// Verifies non-finite <see cref="double" /> values still succeed when the runtime type already matches the <see cref="double" /> target (identity path).
    /// </summary>
    [Fact]
    public void DoubleTargetPreservesNanAndInfinityViaIdentity()
    {
        Assert.True(TypedCacheScalarConverter.TryConvertObject(double.NaN, typeof(double), out var nan));
        Assert.True(double.IsNaN((double)nan));
        Assert.True(TypedCacheScalarConverter.TryConvertObject(double.PositiveInfinity, typeof(double), out var inf));
        Assert.True(double.IsPositiveInfinity((double)inf));
    }

    /// <summary>
    /// Verifies non-finite <see cref="float" /> values are rejected when converted through the strict double bridge.
    /// </summary>
    [Fact]
    public void DoubleTargetRejectsNonFiniteFloatSource()
    {
        Assert.False(TypedCacheScalarConverter.TryConvertObject(float.NaN, typeof(double), out _));
        Assert.False(TypedCacheScalarConverter.TryConvertObject(float.PositiveInfinity, typeof(double), out _));
    }

    /// <summary>
    /// Verifies enum conversion uses strict underlying numeric conversion.
    /// </summary>
    [Fact]
    public void EnumTargetAcceptsMatchingUnderlying()
    {
        Assert.True(TypedCacheScalarConverter.TryConvertObject((byte)1, typeof(ByteBacked), out var o));
        Assert.Equal(ByteBacked.A, o);
    }

    /// <summary>
    /// Verifies in-range finite <see cref="double" /> values convert to <see cref="float" />.
    /// </summary>
    [Fact]
    public void FloatTargetAcceptsFiniteDouble()
    {
        Assert.True(TypedCacheScalarConverter.TryConvertObject(1.25d, typeof(float), out var o));
        Assert.Equal(1.25f, (float)o);
    }

    /// <summary>
    /// Verifies non-finite <see cref="double" /> values are rejected for <see cref="float" /> targets (no direct identity; strict double bridge rejects non-finite).
    /// </summary>
    [Fact]
    public void FloatTargetRejectsNonFiniteDoubleSource()
    {
        Assert.False(TypedCacheScalarConverter.TryConvertObject(double.NaN, typeof(float), out _));
        Assert.False(TypedCacheScalarConverter.TryConvertObject(double.PositiveInfinity, typeof(float), out _));
    }

    /// <summary>
    /// Verifies <see cref="Guid" /> identity conversion succeeds; unrelated types are rejected.
    /// </summary>
    [Fact]
    public void GuidTargetAcceptsGuidOnly()
    {
        var g = Guid.Parse("a1a1a1a1-b2b2-c3c3-d4d4-e5e5e5e5e5e5");
        Assert.True(TypedCacheScalarConverter.TryConvertObject(g, typeof(Guid), out var o));
        Assert.Equal(g, o);
        Assert.False(TypedCacheScalarConverter.TryConvertObject("a1a1a1a1-b2b2-c3c3-d4d4-e5e5e5e5e5e5", typeof(Guid), out _));
    }

    /// <summary>
    /// Verifies integral floating-point values outside decimal range are rejected for integer targets.
    /// </summary>
    [Fact]
    public void IntegerTargetRejectsFloatingPointOverflowBeforeNarrowing()
    {
        Assert.False(TypedCacheScalarConverter.TryConvertObject(double.MaxValue, typeof(long), out _));
        Assert.False(TypedCacheScalarConverter.TryConvertObject(float.MaxValue, typeof(int), out _));
    }

    /// <summary>
    /// Verifies supported integral scalar conversions cover every primitive source family.
    /// </summary>
    /// <param name="value">The source scalar value.</param>
    /// <param name="targetType">The requested target type.</param>
    /// <param name="expected">The expected converted value.</param>
    [Theory]
    [MemberData(nameof(IntegralScalarConversionCases))]
    public void IntegralScalarConversionsUseStrictCheckedRules(object value, Type targetType, object expected)
    {
        Assert.True(TypedCacheScalarConverter.TryConvertObject(value, targetType, out var converted));
        Assert.Equal(expected, converted);
    }

    /// <summary>
    /// Verifies <see cref="int" />-backed enums accept matching underlying integer values.
    /// </summary>
    [Fact]
    public void IntEnumTargetAcceptsUnderlyingInt()
    {
        Assert.True(TypedCacheScalarConverter.TryConvertObject(99, typeof(IntBacked), out var o));
        Assert.Equal(IntBacked.Z, o);
    }

    /// <summary>
    /// Verifies exact runtime type match returns the same boxed value for value types.
    /// </summary>
    [Fact]
    public void IntTargetAcceptsBoxedIntWithSameRuntimeType()
    {
        object boxed = 7;
        Assert.True(TypedCacheScalarConverter.TryConvertObject(boxed, typeof(int), out var o));
        Assert.Equal(7, o);
    }

    /// <summary>
    /// Verifies narrowing from <see cref="long" /> to <see cref="int" /> succeeds when in range.
    /// </summary>
    [Fact]
    public void IntTargetAcceptsNarrowLongInRange()
    {
        Assert.True(TypedCacheScalarConverter.TryConvertObject(42L, typeof(int), out var o));
        Assert.Equal(42, o);
    }

    /// <summary>
    /// Verifies whole <see cref="decimal" /> values convert to integers when in range.
    /// </summary>
    [Fact]
    public void IntTargetAcceptsWholeDecimal()
    {
        Assert.True(TypedCacheScalarConverter.TryConvertObject(42.0m, typeof(int), out var o));
        Assert.Equal(42, o);
    }

    /// <summary>
    /// Verifies fractional <see cref="decimal" /> values are rejected for integer targets.
    /// </summary>
    [Fact]
    public void IntTargetRejectsFractionalDecimal() => Assert.False(TypedCacheScalarConverter.TryConvertObject(1.1m, typeof(int), out _));

    /// <summary>
    /// Verifies fractional <see cref="double" /> values are rejected for integer targets.
    /// </summary>
    [Fact]
    public void IntTargetRejectsFractionalDouble() => Assert.False(TypedCacheScalarConverter.TryConvertObject(1.5d, typeof(int), out _));

    /// <summary>
    /// Verifies fractional <see cref="float" /> values are rejected for integer targets.
    /// </summary>
    [Fact]
    public void IntTargetRejectsFractionalFloat() => Assert.False(TypedCacheScalarConverter.TryConvertObject(2.5f, typeof(int), out _));

    /// <summary>
    /// Verifies whole <see cref="double" /> values outside <see cref="int" /> range are rejected for <see cref="int" /> targets.
    /// </summary>
    [Fact]
    public void IntTargetRejectsIntegerLikeDoubleOutsideIntRange() => Assert.False(TypedCacheScalarConverter.TryConvertObject(int.MaxValue + 1.0d, typeof(int), out _));

    /// <summary>
    /// Verifies invalid numeric strings are rejected the same way as numeric-looking strings.
    /// </summary>
    /// <param name="value">The string value to convert.</param>
    [Theory]
    [InlineData("")]
    [InlineData("not-a-number")]
    [InlineData("1.25")]
    public void IntTargetRejectsInvalidNumericStrings(string value) => Assert.False(TypedCacheScalarConverter.TryConvertObject(value, typeof(int), out _));

    /// <summary>
    /// Verifies narrowing from <see cref="long" /> to <see cref="int" /> fails outside <see cref="int" /> range.
    /// </summary>
    [Fact]
    public void IntTargetRejectsLongOverflow() => Assert.False(TypedCacheScalarConverter.TryConvertObject(int.MaxValue + 1L, typeof(int), out _));

    /// <summary>
    /// Verifies numeric strings are not coerced into integers.
    /// </summary>
    [Fact]
    public void IntTargetRejectsParsableString() => Assert.False(TypedCacheScalarConverter.TryConvertObject("42", typeof(int), out _));

    /// <summary>
    /// Verifies JsonElement array kind is rejected for scalar numeric targets.
    /// </summary>
    [Fact]
    public void JsonElementArrayRejectsScalarTargets()
    {
        using var doc = JsonDocument.Parse("[1,2,3]");
        Assert.False(TypedCacheScalarConverter.TryConvertObject(doc.RootElement, typeof(long), out _));
        Assert.False(TypedCacheScalarConverter.TryConvertObject(doc.RootElement, typeof(double), out _));
    }

    /// <summary>
    /// Verifies <see cref="JsonElement" /> only converts to <see cref="JsonElement" /> via identity; numeric kinds are not coerced.
    /// </summary>
    [Fact]
    public void JsonElementDoesNotCoerceToNumeric()
    {
        using var doc = JsonDocument.Parse("42");
        var root = doc.RootElement;
        Assert.True(TypedCacheScalarConverter.TryConvertObject(root, typeof(JsonElement), out var same));
        Assert.Equal(JsonValueKind.Number, Assert.IsType<JsonElement>(same).ValueKind);
        Assert.False(TypedCacheScalarConverter.TryConvertObject(root, typeof(int), out _));
    }

    /// <summary>
    /// Verifies JSON null element does not convert to reference targets without identity.
    /// </summary>
    [Fact]
    public void JsonElementNullKindRejectsStringTarget()
    {
        using var doc = JsonDocument.Parse("null");
        Assert.False(TypedCacheScalarConverter.TryConvertObject(doc.RootElement, typeof(string), out _));
    }

    /// <summary>
    /// Verifies JSON object elements are rejected for integer scalar targets.
    /// </summary>
    [Fact]
    public void JsonElementObjectRejectsIntegerTarget()
    {
        using var doc = JsonDocument.Parse("{\"n\":1}");
        Assert.False(TypedCacheScalarConverter.TryConvertObject(doc.RootElement, typeof(int), out _));
    }

    /// <summary>
    /// Verifies JsonElement string and object kinds are not coerced into integer scalars.
    /// </summary>
    [Fact]
    public void JsonElementStringAndObjectRejectIntegerTarget()
    {
        using var stringDoc = JsonDocument.Parse("\"42\"");
        using var objectDoc = JsonDocument.Parse("{\"n\":42}");
        Assert.False(TypedCacheScalarConverter.TryConvertObject(stringDoc.RootElement, typeof(int), out _));
        Assert.False(TypedCacheScalarConverter.TryConvertObject(objectDoc.RootElement, typeof(int), out _));
    }

    /// <summary>
    /// Verifies whole-number <see cref="double" /> values are accepted for <see cref="long" /> targets when in range.
    /// </summary>
    [Fact]
    public void LongTargetAcceptsIntegerLikeDouble()
    {
        Assert.True(TypedCacheScalarConverter.TryConvertObject(123.0d, typeof(long), out var o));
        Assert.Equal(123L, o);
    }

    /// <summary>
    /// Verifies widening from <see cref="int" /> to <see cref="long" /> succeeds.
    /// </summary>
    [Fact]
    public void LongTargetAcceptsWidenedInt()
    {
        Assert.True(TypedCacheScalarConverter.TryConvertObject(123, typeof(long), out var o));
        Assert.Equal(123L, o);
    }

    /// <summary>
    /// Verifies <see cref="ulong" /> values outside <see cref="long" /> range are rejected.
    /// </summary>
    [Fact]
    public void LongTargetRejectsUlongOverflow() => Assert.False(TypedCacheScalarConverter.TryConvertObject(ulong.MaxValue, typeof(long), out _));

    /// <summary>
    /// Verifies nullable boolean wrappers are constructed from a non-null <see cref="bool" />.
    /// </summary>
    [Fact]
    public void NullableBoolTargetAcceptsBool()
    {
        Assert.True(TypedCacheScalarConverter.TryConvertObject(false, typeof(bool?), out var o));
        Assert.False((bool)o);
    }

    /// <summary>
    /// Verifies nullable integer targets reject floating-point values with fractional parts.
    /// </summary>
    [Fact]
    public void NullableIntegerRejectsFractionalFloatingPoint()
    {
        Assert.False(TypedCacheScalarConverter.TryConvertObject(10.5d, typeof(int?), out _));
        Assert.False(TypedCacheScalarConverter.TryConvertObject(3.25f, typeof(long?), out _));
    }

    /// <summary>
    /// Verifies nullable value types are boxed with the correct nullable wrapper.
    /// </summary>
    [Fact]
    public void NullableIntTargetAcceptsByte()
    {
        Assert.True(TypedCacheScalarConverter.TryConvertObject((byte)5, typeof(int?), out var o));
        Assert.Equal(5, Assert.IsType<int>(o));
    }

    /// <summary>
    /// Verifies <c>object</c> target accepts any non-null value without copying.
    /// </summary>
    [Fact]
    public void ObjectTargetReturnsSameInstance()
    {
        var payload = new object();
        Assert.True(TypedCacheScalarConverter.TryConvertObject(payload, typeof(object), out var o));
        Assert.Same(payload, o);
    }

    /// <summary>
    /// Verifies <see cref="string" /> target only accepts a <see cref="string" /> instance.
    /// </summary>
    [Fact]
    public void StringTargetAcceptsStringAndRejectsInt()
    {
        Assert.True(TypedCacheScalarConverter.TryConvertObject("x", typeof(string), out var s));
        Assert.Equal("x", s);
        Assert.False(TypedCacheScalarConverter.TryConvertObject(1, typeof(string), out _));
    }

    /// <summary>
    /// Verifies <see cref="ArgumentNullException" /> is thrown for a null requested type.
    /// </summary>
    [Fact]
    public void TryConvertObjectThrowsForNullRequestedType() => _ = Assert.Throws<ArgumentNullException>(static () => TypedCacheScalarConverter.TryConvertObject(1, null!, out _));

    /// <summary>
    /// Verifies <see cref="ArgumentNullException" /> is thrown for a null value argument.
    /// </summary>
    [Fact]
    public void TryConvertObjectThrowsForNullValue() =>
        _ = Assert.Throws<ArgumentNullException>(static () => TypedCacheScalarConverter.TryConvertObject(null!, typeof(int), out _));

    /// <summary>
    /// Verifies negative values are rejected for unsigned integer targets.
    /// </summary>
    [Fact]
    public void UIntTargetRejectsNegativeInt() => Assert.False(TypedCacheScalarConverter.TryConvertObject(-1, typeof(uint), out _));

    /// <summary>
    /// Verifies <see cref="long" /> widening is accepted for <see cref="ulong" /> when non-negative.
    /// </summary>
    [Fact]
    public void ULongTargetAcceptsNonNegativeLong()
    {
        Assert.True(TypedCacheScalarConverter.TryConvertObject(42L, typeof(ulong), out var o));
        Assert.Equal(42UL, o);
    }

    /// <summary>
    /// Verifies <see cref="long" /> negative values are rejected for <see cref="ulong" /> targets.
    /// </summary>
    [Fact]
    public void ULongTargetRejectsNegativeLong() => Assert.False(TypedCacheScalarConverter.TryConvertObject(-1L, typeof(ulong), out _));

    /// <summary>
    /// Verifies unsupported target types fail via a false conversion result instead of silent coercion.
    /// </summary>
    [Fact]
    public void UnsupportedTargetTypeReturnsFalseWithNullConvertedValue()
    {
        Assert.False(TypedCacheScalarConverter.TryConvertObject("2026-05-06", typeof(DateOnly), out var converted));
        Assert.Null(converted);
    }

    /// <summary>
    /// Verifies <see cref="ushort" /> targets accept in-range signed integers.
    /// </summary>
    [Fact]
    public void UShortTargetAcceptsNarrowIntInRange()
    {
        Assert.True(TypedCacheScalarConverter.TryConvertObject((short)100, typeof(ushort), out var o));
        Assert.Equal((ushort)100, o);
    }
}
