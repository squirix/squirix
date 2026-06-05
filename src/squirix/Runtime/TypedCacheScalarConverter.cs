using System;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;

namespace Squirix.Runtime;

/// <summary>
/// Strict scalar conversion helpers for typed cache values: rejects silent rounding of fractional
/// floating-point values into integer targets and blocks string-to-integer coercion.
/// </summary>
internal static class TypedCacheScalarConverter
{
    /// <summary>
    /// Attempts to convert <paramref name="value" /> to an instance compatible with <paramref name="requestedType" />
    /// (including nullable value types).
    /// </summary>
    /// <param name="value">The non-null runtime value stored in the untyped cache.</param>
    /// <param name="requestedType">The requested CLR type (can be nullable).</param>
    /// <param name="converted">The converted value boxed as <paramref name="requestedType" /> when the method returns <c>true</c>.</param>
    /// <returns><c>true</c> when conversion succeeds; otherwise <c>false</c>.</returns>
    public static bool TryConvertObject(object value, Type requestedType, [NotNullWhen(true)] out object? converted)
    {
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(requestedType);

        if (requestedType == typeof(object))
        {
            converted = value;
            return true;
        }

        var nullableUnderlying = Nullable.GetUnderlyingType(requestedType);
        var nonNullable = nullableUnderlying ?? requestedType;

        if (value.GetType() == requestedType)
        {
            converted = value;
            return true;
        }

        if (nullableUnderlying is not null && value.GetType() == nullableUnderlying)
        {
            converted = Activator.CreateInstance(requestedType, value)!;
            return true;
        }

        if (nonNullable.IsEnum)
        {
            if (!TryConvertToEnumStrict(value, nonNullable, out var enumObj))
            {
                converted = null;
                return false;
            }

            converted = nullableUnderlying is not null ? Activator.CreateInstance(requestedType, enumObj)! : enumObj;
            return true;
        }

        if (!TryConvertNonEnumScalar(value, nonNullable, out var core))
        {
            converted = null;
            return false;
        }

        converted = nullableUnderlying is not null ? Activator.CreateInstance(requestedType, core)! : core;
        return true;
    }

    private static bool IsIntegerType(Type t) => t == typeof(byte) || t == typeof(sbyte) || t == typeof(short) || t == typeof(ushort) || t == typeof(int) || t == typeof(uint) ||
                                                 t == typeof(long) || t == typeof(ulong);

    private static bool TryConvertNonEnumScalar(object value, Type target, [NotNullWhen(true)] out object? converted)
    {
        if (target == typeof(string))
        {
            if (value is string s)
            {
                converted = s;
                return true;
            }

            converted = null;
            return false;
        }

        if (target == typeof(bool))
        {
            if (value is bool b)
            {
                converted = b;
                return true;
            }

            converted = null;
            return false;
        }

        if (target == typeof(char))
        {
            if (value is char c)
            {
                converted = c;
                return true;
            }

            converted = null;
            return false;
        }

        if (IsIntegerType(target))
        {
            if (TryNormalizeToIntegralDecimal(value, out var integral))
                return TryDecimalToIntegerTarget(integral, target, out converted);
            converted = null;
            return false;
        }

        if (target == typeof(double))
        {
            if (!TryToDoubleStrict(value, out var d))
            {
                converted = null;
                return false;
            }

            converted = d;
            return true;
        }

        if (target == typeof(float))
        {
            if (!TryToFloatStrict(value, out var f))
            {
                converted = null;
                return false;
            }

            converted = f;
            return true;
        }

        if (target == typeof(decimal))
        {
            if (!TryToDecimalStrict(value, out var m))
            {
                converted = null;
                return false;
            }

            converted = m;
            return true;
        }

        converted = null;
        return false;
    }

    private static bool TryConvertToEnumStrict(object value, Type enumType, [NotNullWhen(true)] out object? enumObj)
    {
        var underlying = Enum.GetUnderlyingType(enumType);
        if (!TryConvertNonEnumScalar(value, underlying, out var boxedUnderlying))
        {
            enumObj = null;
            return false;
        }

        try
        {
            enumObj = Enum.ToObject(enumType, boxedUnderlying);
            return true;
        }
        catch (ArgumentException)
        {
            enumObj = null;
            return false;
        }
    }

    private static bool TryDecimalToIntegerTarget(decimal integral, Type target, [NotNullWhen(true)] out object? converted)
    {
        try
        {
            if (target == typeof(byte))
            {
                converted = decimal.ToByte(integral);
                return true;
            }

            if (target == typeof(sbyte))
            {
                converted = decimal.ToSByte(integral);
                return true;
            }

            if (target == typeof(short))
            {
                converted = decimal.ToInt16(integral);
                return true;
            }

            if (target == typeof(ushort))
            {
                converted = decimal.ToUInt16(integral);
                return true;
            }

            if (target == typeof(int))
            {
                converted = decimal.ToInt32(integral);
                return true;
            }

            if (target == typeof(uint))
            {
                converted = decimal.ToUInt32(integral);
                return true;
            }

            if (target == typeof(long))
            {
                converted = decimal.ToInt64(integral);
                return true;
            }

            if (target == typeof(ulong))
            {
                converted = decimal.ToUInt64(integral);
                return true;
            }
        }
        catch (OverflowException)
        {
            converted = null;
            return false;
        }

        converted = null;
        return false;
    }

    private static bool TryDoubleCreateChecked<T>(T value, out double converted)
        where T : IBinaryInteger<T>
    {
        try
        {
            converted = double.CreateChecked(value);
            return true;
        }
        catch (OverflowException)
        {
            converted = 0;
            return false;
        }
    }

    private static bool TryNormalizeToIntegralDecimal(object value, out decimal integral)
    {
        switch (value)
        {
            case byte x:
                integral = x;
                return true;
            case sbyte x:
                integral = x;
                return true;
            case short x:
                integral = x;
                return true;
            case ushort x:
                integral = x;
                return true;
            case int x:
                integral = x;
                return true;
            case uint x:
                integral = x;
                return true;
            case long x:
                integral = x;
                return true;
            case ulong x:
                integral = decimal.CreateChecked(x);
                return true;
            case double d:
                if (!double.IsInteger(d))
                {
                    integral = 0;
                    return false;
                }

                try
                {
                    integral = decimal.CreateChecked(d);
                    return true;
                }
                catch (OverflowException)
                {
                    integral = 0;
                    return false;
                }

            case float f:
                if (!float.IsInteger(f))
                {
                    integral = 0;
                    return false;
                }

                try
                {
                    integral = decimal.CreateChecked(f);
                    return true;
                }
                catch (OverflowException)
                {
                    integral = 0;
                    return false;
                }

            case decimal dec:
                if (dec != decimal.Truncate(dec))
                {
                    integral = 0;
                    return false;
                }

                integral = dec;
                return true;
            default:
                integral = 0;
                return false;
        }
    }

    private static bool TryToDecimalStrict(object value, out decimal converted)
    {
        switch (value)
        {
            case decimal m:
                converted = m;
                return true;
            case byte x:
                converted = x;
                return true;
            case sbyte x:
                converted = x;
                return true;
            case short x:
                converted = x;
                return true;
            case ushort x:
                converted = x;
                return true;
            case int x:
                converted = x;
                return true;
            case uint x:
                converted = x;
                return true;
            case long x:
                converted = x;
                return true;
            case ulong x:
                converted = decimal.CreateChecked(x);
                return true;
            case double d:
                if (!double.IsFinite(d))
                {
                    converted = 0;
                    return false;
                }

                try
                {
                    converted = decimal.CreateChecked(d);
                    return true;
                }
                catch (OverflowException)
                {
                    converted = 0;
                    return false;
                }

            case float f:
                if (!float.IsFinite(f))
                {
                    converted = 0;
                    return false;
                }

                try
                {
                    converted = decimal.CreateChecked(f);
                    return true;
                }
                catch (OverflowException)
                {
                    converted = 0;
                    return false;
                }

            default:
                converted = 0;
                return false;
        }
    }

    private static bool TryToDoubleStrict(object value, out double converted)
    {
        switch (value)
        {
            case double d:
                if (!double.IsFinite(d))
                {
                    converted = 0;
                    return false;
                }

                converted = d;
                return true;
            case float f:
                if (!float.IsFinite(f))
                {
                    converted = 0;
                    return false;
                }

                converted = f;
                return true;
            case decimal m:
                try
                {
                    converted = double.CreateChecked(m);
                    return true;
                }
                catch (OverflowException)
                {
                    converted = 0;
                    return false;
                }

            case byte x:
                converted = x;
                return true;
            case sbyte x:
                converted = x;
                return true;
            case short x:
                converted = x;
                return true;
            case ushort x:
                converted = x;
                return true;
            case int x:
                converted = x;
                return true;
            case uint x:
                converted = x;
                return true;

            default:
                switch (value)
                {
                    case long lx:
                        return TryDoubleCreateChecked(lx, out converted);
                    case ulong ux:
                        return TryDoubleCreateChecked(ux, out converted);
                    default:
                        converted = 0;
                        return false;
                }
        }
    }

    private static bool TryToFloatStrict(object value, out float converted)
    {
        if (!TryToDoubleStrict(value, out var d))
        {
            converted = 0;
            return false;
        }

        try
        {
            converted = float.CreateChecked(d);
            return true;
        }
        catch (OverflowException)
        {
            converted = 0;
            return false;
        }
    }
}
