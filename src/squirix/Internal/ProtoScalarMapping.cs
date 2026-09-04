using System.Runtime.CompilerServices;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Internal;

/// <summary>Maps CLR scalar and typed-primitive cache values to and from protobuf representations.</summary>
internal static class ProtoScalarMapping
{
    internal static T? Coerce<T>(object? value) => value is T result ? result : default;

    internal static bool IsTypedPrimitiveKind(CacheValue.KindOneofCase kind) => kind == CacheValue.KindOneofCase.StringValue || kind == CacheValue.KindOneofCase.BoolValue
        || kind == CacheValue.KindOneofCase.Int32Value || kind == CacheValue.KindOneofCase.Int64Value || kind == CacheValue.KindOneofCase.DoubleValue;

    internal static bool TryMapTypedPrimitive<T>(CacheValue value, out T? result)
    {
        result = default;
        return value.KindCase switch
        {
            CacheValue.KindOneofCase.StringValue => TryMapString(value, out result),
            CacheValue.KindOneofCase.BoolValue => TryMapBool(value, out result),
            CacheValue.KindOneofCase.Int32Value => TryMapInt32(value, out result),
            CacheValue.KindOneofCase.Int64Value => TryMapInt64(value, out result),
            CacheValue.KindOneofCase.DoubleValue => TryMapDouble(value, out result),
            _ => false,
        };
    }

    private static TTarget ReinterpretReference<TTarget, TValue>(TValue value)
        where TValue : class?
    {
        var reference = value;
        return Unsafe.As<TValue, TTarget>(ref reference);
    }

    private static TTarget ReinterpretScalar<TTarget, TValue>(TValue value)
        where TValue : struct => Unsafe.As<TValue, TTarget>(ref value);

    private static bool TryMapBool<T>(CacheValue value, out T? result)
    {
        if (typeof(T) == typeof(bool))
        {
            result = ReinterpretScalar<T, bool>(value.BoolValue);
            return true;
        }

        result = default;
        return false;
    }

    private static bool TryMapDouble<T>(CacheValue value, out T? result)
    {
        if (typeof(T) == typeof(double))
        {
            result = ReinterpretScalar<T, double>(value.DoubleValue);
            return true;
        }

        result = default;
        return false;
    }

    private static bool TryMapInt32<T>(CacheValue value, out T? result)
    {
        if (typeof(T) == typeof(int))
        {
            result = ReinterpretScalar<T, int>(int.CreateChecked(value.Int32Value));
            return true;
        }

        result = default;
        return false;
    }

    private static bool TryMapInt64<T>(CacheValue value, out T? result)
    {
        if (typeof(T) == typeof(long))
        {
            result = ReinterpretScalar<T, long>(value.Int64Value);
            return true;
        }

        result = default;
        return false;
    }

    private static bool TryMapString<T>(CacheValue value, out T? result)
    {
        if (typeof(T) == typeof(string))
        {
            result = ReinterpretReference<T, string>(value.StringValue);
            return true;
        }

        result = default;
        return false;
    }
}
