using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace Protobus.Internal;

/// <summary>
/// Tolerant readers for AMQP header values, accepting every encoding peers produce: integers of
/// any width or as decimal text, booleans as a boolean, number or text. RabbitMQ.Client hands a
/// long string over as <c>byte[]</c>.
/// </summary>
public static class Headers
{
    public static string? Text(object? v) => v switch
    {
        null => null,
        byte[] bytes => Encoding.UTF8.GetString(bytes),
        string s => s,
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => v.ToString(),
    };

    /// <summary>An integer, or null when absent or unparseable.</summary>
    public static long? Integer(object? v)
    {
        switch (v)
        {
            case null: return null;
            case sbyte or byte or short or ushort or int or uint or long: return Convert.ToInt64(v, CultureInfo.InvariantCulture);
            case ulong u: return u <= long.MaxValue ? (long)u : null;
            case float or double or decimal:
                var d = Convert.ToDouble(v, CultureInfo.InvariantCulture);
                return d == Math.Round(d) && !double.IsInfinity(d) ? (long)d : null;
        }
        var t = Text(v);
        return long.TryParse(t?.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) ? n : null;
    }

    public static bool Bool(object? v)
    {
        switch (v)
        {
            case null: return false;
            case bool b: return b;
            case sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal:
                return Convert.ToDouble(v, CultureInfo.InvariantCulture) != 0;
        }
        var t = Text(v);
        return t != null && (t.Equals("true", StringComparison.OrdinalIgnoreCase) || t == "1");
    }

    public static object? Get(IDictionary<string, object?>? headers, string key) =>
        headers != null && headers.TryGetValue(key, out var v) ? v : null;
}
