using System;
using System.IO;
using System.Text;

namespace Protobus.Internal;

/// <summary>
/// The protobus envelopes: the five small protobuf messages every request, reply and event
/// travels inside. In the TypeScript reference they are protobufjs decorator classes, so
/// presence works proto2-style: an empty string or empty bytes is still written. This codec
/// reproduces those bytes exactly rather than leaning on proto3 generated code, which would omit
/// them. Both forms decode identically on every port.
/// </summary>
/// <remarks>
/// <code>
///   message RequestContainer  { string method = 1; string actor = 2; bytes data = 3; }
///   message ResponseResult    { string method = 1; bytes data = 2; }
///   message ResponseError     { string method = 1; string message = 2; string code = 3; }
///   message ResponseContainer { oneof value { ResponseResult result = 1; ResponseError error = 2; } }
///   message EventContainer    { string type = 1; string topic = 2; bytes data = 3; }
/// </code>
/// </remarks>
public static class Envelopes
{
    /// <summary>A request. <c>Actor</c> is null when the caller named none: then it is not written, as TypeScript does.</summary>
    public sealed record Request(string Method, string? Actor, byte[] Data);

    public sealed record Result(string Method, byte[] Data);

    /// <summary>Only these three fields cross the wire.</summary>
    public sealed record ErrorReply(string Method, string Message, string Code);

    /// <summary>Exactly one of <see cref="Result"/> and <see cref="Error"/> is set.</summary>
    public sealed record Response(Result? Result, ErrorReply? Error);

    public sealed record Event(string Type, string Topic, byte[] Data);

    /// <summary>Bytes that are not a valid envelope.</summary>
    public sealed class MalformedException : Exception
    {
        public MalformedException(string what) : base("protobus: malformed envelope: " + what) { }
    }

    // ---- encoding ----------------------------------------------------------------------

    /// <summary>The method and data are always written; the actor only when there is one.</summary>
    public static byte[] EncodeRequest(Request r)
    {
        var w = new Writer();
        w.String(1, r.Method);
        if (r.Actor != null) w.String(2, r.Actor);
        w.Bytes(3, r.Data);
        return w.ToArray();
    }

    /// <exception cref="ArgumentException">unless exactly one member is set</exception>
    public static byte[] EncodeResponse(Response r)
    {
        if ((r.Result == null) == (r.Error == null)) throw new ArgumentException("a response carries exactly one of result and error");
        var w = new Writer();
        if (r.Result != null)
        {
            w.Bytes(1, EncodeResult(r.Result));
        }
        else
        {
            var e = new Writer();
            e.String(1, r.Error!.Method);
            e.String(2, r.Error.Message);
            e.String(3, r.Error.Code);
            w.Bytes(2, e.ToArray());
        }
        return w.ToArray();
    }

    /// <summary>A bare ResponseResult, not wrapped in a container.</summary>
    public static byte[] EncodeResult(Result r)
    {
        var w = new Writer();
        w.String(1, r.Method);
        w.Bytes(2, r.Data);
        return w.ToArray();
    }

    /// <summary>All three fields are always written.</summary>
    public static byte[] EncodeEvent(Event e)
    {
        var w = new Writer();
        w.String(1, e.Type);
        w.String(2, e.Topic);
        w.Bytes(3, e.Data);
        return w.ToArray();
    }

    // ---- decoding ----------------------------------------------------------------------

    public static Request DecodeRequest(byte[] b)
    {
        var r = new Reader(b);
        string method = "", actor = "";
        byte[] data = Array.Empty<byte>();
        while (r.More)
        {
            var tag = r.Tag();
            switch (tag >> 3)
            {
                case 1: method = r.String(tag); break;
                case 2: actor = r.String(tag); break;
                case 3: data = r.Bytes(tag); break;
                default: r.Skip(tag); break;
            }
        }
        return new Request(method, actor, data);
    }

    /// <summary>
    /// When both members are present the error wins, matching the TypeScript reader, so the same
    /// bytes cannot mean success on one port and failure on another. Neither is refused.
    /// </summary>
    public static Response DecodeResponse(byte[] b)
    {
        var r = new Reader(b);
        Result? result = null;
        ErrorReply? error = null;
        while (r.More)
        {
            var tag = r.Tag();
            switch (tag >> 3)
            {
                case 1: result = DecodeResult(r.Bytes(tag)); break;
                case 2: error = DecodeError(r.Bytes(tag)); break;
                default: r.Skip(tag); break;
            }
        }
        if (error != null) return new Response(null, error);
        if (result != null) return new Response(result, null);
        throw new MalformedException("response carries neither a result nor an error");
    }

    private static Result DecodeResult(byte[] b)
    {
        var r = new Reader(b);
        string method = "";
        byte[] data = Array.Empty<byte>();
        while (r.More)
        {
            var tag = r.Tag();
            switch (tag >> 3)
            {
                case 1: method = r.String(tag); break;
                case 2: data = r.Bytes(tag); break;
                default: r.Skip(tag); break;
            }
        }
        return new Result(method, data);
    }

    private static ErrorReply DecodeError(byte[] b)
    {
        var r = new Reader(b);
        string method = "", message = "", code = "";
        while (r.More)
        {
            var tag = r.Tag();
            switch (tag >> 3)
            {
                case 1: method = r.String(tag); break;
                case 2: message = r.String(tag); break;
                case 3: code = r.String(tag); break;
                default: r.Skip(tag); break;
            }
        }
        return new ErrorReply(method, message, code);
    }

    public static Event DecodeEvent(byte[] b)
    {
        var r = new Reader(b);
        string type = "", topic = "";
        byte[] data = Array.Empty<byte>();
        while (r.More)
        {
            var tag = r.Tag();
            switch (tag >> 3)
            {
                case 1: type = r.String(tag); break;
                case 2: topic = r.String(tag); break;
                case 3: data = r.Bytes(tag); break;
                default: r.Skip(tag); break;
            }
        }
        return new Event(type, topic, data);
    }

    // ---- wire helpers ------------------------------------------------------------------

    internal sealed class Writer
    {
        private readonly MemoryStream out_ = new();

        private void Varint(ulong v)
        {
            while (v >= 0x80)
            {
                out_.WriteByte((byte)(v | 0x80));
                v >>= 7;
            }
            out_.WriteByte((byte)v);
        }

        internal void String(int field, string? value) => Bytes(field, Encoding.UTF8.GetBytes(value ?? ""));

        internal void Bytes(int field, byte[]? value)
        {
            var v = value ?? Array.Empty<byte>();
            Varint(((ulong)field << 3) | 2);
            Varint((ulong)v.Length);
            out_.Write(v, 0, v.Length);
        }

        internal byte[] ToArray() => out_.ToArray();
    }

    internal sealed class Reader
    {
        private static readonly UTF8Encoding StrictUtf8 = new(false, true);
        private readonly byte[] b;
        private int pos;

        internal Reader(byte[]? b) => this.b = b ?? Array.Empty<byte>();

        internal bool More => pos < b.Length;

        private ulong Varint()
        {
            ulong result = 0;
            for (var shift = 0; shift < 64; shift += 7)
            {
                if (pos >= b.Length) throw new MalformedException("truncated varint");
                var x = b[pos++];
                result |= (ulong)(x & 0x7F) << shift;
                if ((x & 0x80) == 0) return result;
            }
            throw new MalformedException("varint longer than ten bytes");
        }

        internal int Tag()
        {
            var t = Varint();
            if (t >> 3 == 0 || t > 0xFFFFFFFF) throw new MalformedException("invalid field tag " + t);
            return (int)(uint)t;
        }

        internal byte[] Bytes(int tag)
        {
            if ((tag & 7) != 2) throw new MalformedException($"field {tag >> 3} has wire type {tag & 7}, not 2");
            var n = Varint();
            if (n > (ulong)(b.Length - pos)) throw new MalformedException($"length {n} overruns the message");
            var v = new byte[(int)n];
            Array.Copy(b, pos, v, 0, (int)n);
            pos += (int)n;
            return v;
        }

        internal string String(int tag)
        {
            var raw = Bytes(tag);
            try
            {
                return StrictUtf8.GetString(raw);
            }
            catch (DecoderFallbackException)
            {
                throw new MalformedException($"field {tag >> 3} is not valid UTF-8");
            }
        }

        internal void Skip(int tag)
        {
            switch (tag & 7)
            {
                case 0: Varint(); break;
                case 1: Advance(8); break;
                case 2: Bytes(tag); break;
                case 5: Advance(4); break;
                default: throw new MalformedException("unsupported wire type " + (tag & 7));
            }
        }

        private void Advance(int n)
        {
            if (n > b.Length - pos) throw new MalformedException("truncated field");
            pos += n;
        }
    }
}
