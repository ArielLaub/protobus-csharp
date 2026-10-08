using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Numerics;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Protobus.Types;

namespace Protobus;

/// <summary>
/// The built-in custom types, <c>bigint</c> and <c>timestamp</c>: one-field messages declared at
/// the root of the type namespace, as in every port. In C# they are
/// <see cref="Protobus.Types.bigint"/> and <see cref="Protobus.Types.timestamp"/>, converted with
/// these helpers to <see cref="BigInteger"/> and <see cref="DateTimeOffset"/>.
/// </summary>
/// <remarks>
/// Every port refuses a negative or oversized bigint and refuses to decode one wider than 32
/// bytes; a timestamp is refused beyond ±8.64e15 ms. protobus checks every request, reply and
/// event it encodes or decodes.
/// </remarks>
public static class CustomTypes
{
    public const int BigintBytes = 32;
    public static readonly BigInteger BigintMax = (BigInteger.One << 256) - 1;
    public const long MaxTimestampMs = 8_640_000_000_000_000L;

    // ---- bigint ------------------------------------------------------------------------

    /// <summary>An unsigned integer as exactly 32 big-endian bytes.</summary>
    public static bigint Bigint(BigInteger value) => new() { Value = ByteString.CopyFrom(BigintBytesOf(value)) };

    public static bigint Bigint(long value) => Bigint(new BigInteger(value));

    /// <summary>Parse decimal, or hexadecimal with a <c>0x</c> prefix.</summary>
    public static bigint Bigint(string value)
    {
        var v = value.Trim();
        BigInteger parsed;
        if (v.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
        {
            if (!BigInteger.TryParse("0" + v.Substring(2), System.Globalization.NumberStyles.HexNumber, null, out parsed))
                throw new CustomTypeRangeError($"bigint value '{value}' is not a decimal or 0x-hex integer");
        }
        else if (!BigInteger.TryParse(v, System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out parsed))
        {
            throw new CustomTypeRangeError($"bigint value '{value}' is not a decimal or 0x-hex integer");
        }
        return Bigint(parsed);
    }

    /// <summary>The value; an unset or empty bigint is zero.</summary>
    public static BigInteger ToBigInteger(bigint? value) =>
        value == null || !value.HasValue ? BigInteger.Zero : FromBytes(value.Value.ToByteArray());

    internal static byte[] BigintBytesOf(BigInteger value)
    {
        if (value.Sign < 0) throw new CustomTypeRangeError($"bigint value {value} is negative; the protobus bigint wire format is unsigned (0 .. 2^256-1)");
        if (value > BigintMax) throw new CustomTypeRangeError($"bigint value {value} exceeds the maximum representable value 2^256-1");
        var raw = value.ToByteArray(isUnsigned: true, isBigEndian: true);
        var out_ = new byte[BigintBytes];
        Array.Copy(raw, 0, out_, BigintBytes - raw.Length, raw.Length);
        return out_;
    }

    internal static BigInteger FromBytes(byte[] data)
    {
        if (data.Length > BigintBytes)
            throw new CustomTypeRangeError($"bigint wire value is {data.Length} bytes; the protobus bigint wire format is at most {BigintBytes}");
        return data.Length == 0 ? BigInteger.Zero : new BigInteger(data, isUnsigned: true, isBigEndian: true);
    }

    // ---- timestamp ---------------------------------------------------------------------

    public static timestamp Timestamp(DateTimeOffset instant) => TimestampMillis(instant.ToUnixTimeMilliseconds());

    public static timestamp TimestampMillis(long epochMillis)
    {
        CheckMillis(epochMillis);
        return new timestamp { Value = epochMillis };
    }

    /// <summary>The instant; an unset timestamp is the epoch.</summary>
    public static DateTimeOffset ToDateTimeOffset(timestamp? value) => DateTimeOffset.FromUnixTimeMilliseconds(ToMillis(value));

    public static long ToMillis(timestamp? value)
    {
        var ms = value?.Value ?? 0;
        CheckMillis(ms);
        return ms;
    }

    private static void CheckMillis(long ms)
    {
        if (ms > MaxTimestampMs || ms < -MaxTimestampMs)
            throw new CustomTypeRangeError($"timestamp value {ms} ms is beyond ±8.64e15 ms, the range every port can represent");
    }

    // ---- validation --------------------------------------------------------------------

    private static readonly ConcurrentDictionary<MessageDescriptor, bool> ContainsCustom = new();

    /// <summary>Check every bigint and timestamp in a message, recursively.</summary>
    /// <exception cref="CustomTypeRangeError">on the first value no port could represent</exception>
    public static void Validate(IMessage? message)
    {
        if (message == null || !Reaches(message.Descriptor)) return;
        Walk(message);
    }

    private static void Walk(IMessage message)
    {
        var type = message.Descriptor;
        if (type.FullName == "bigint")
        {
            CheckBigint(message);
            return;
        }
        if (type.FullName == "timestamp")
        {
            CheckTimestamp(message);
            return;
        }
        foreach (var field in type.Fields.InFieldNumberOrder())
        {
            if (field.FieldType != FieldType.Message) continue;
            var value = field.Accessor.GetValue(message);
            if (value == null) continue;
            if (field.IsMap)
            {
                var valueField = field.MessageType.FindFieldByNumber(2);
                if (valueField.FieldType != FieldType.Message || !Reaches(valueField.MessageType)) continue;
                foreach (DictionaryEntry e in (IDictionary)value) if (e.Value is IMessage m) Walk(m);
            }
            else if (field.IsRepeated)
            {
                if (!Reaches(field.MessageType)) continue;
                foreach (var item in (IEnumerable)value) if (item is IMessage m) Walk(m);
            }
            else if (Reaches(field.MessageType) && value is IMessage m)
            {
                Walk(m);
            }
        }
    }

    private static void CheckBigint(IMessage m)
    {
        var field = m.Descriptor.FindFieldByNumber(1);
        if (field == null || field.FieldType != FieldType.Bytes) return;
        if (field.HasPresence && !field.Accessor.HasValue(m)) return;
        if (field.Accessor.GetValue(m) is ByteString bytes) FromBytes(bytes.ToByteArray());
    }

    private static void CheckTimestamp(IMessage m)
    {
        var field = m.Descriptor.FindFieldByNumber(1);
        if (field == null || field.FieldType != FieldType.Int64) return;
        if (field.Accessor.GetValue(m) is long ms) CheckMillis(ms);
    }

    /// <summary>Whether values of this type can contain a custom type, cycles included.</summary>
    internal static bool Reaches(MessageDescriptor type) =>
        ContainsCustom.GetOrAdd(type, t => Reaches(t, new HashSet<string>()));

    private static bool Reaches(MessageDescriptor type, HashSet<string> visiting)
    {
        if (type.FullName is "bigint" or "timestamp") return true;
        if (!visiting.Add(type.FullName)) return false;
        foreach (var field in type.Fields.InFieldNumberOrder())
        {
            if (field.FieldType != FieldType.Message) continue;
            var target = field.IsMap ? field.MessageType : field.MessageType;
            if (Reaches(target, visiting)) return true;
        }
        return false;
    }
}
