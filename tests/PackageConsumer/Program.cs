using Calculator;
using Protobus;
using Protobus.Testing;

var broker = new MemoryBroker();
await using var ctx = new Context(new ContextOptions { Transport = broker });
await ctx.InitAsync("amqp://memory/");
await using var service = new Calc(ctx);
await service.InitAsync();
var calc = new ServiceProtobus.Proxy(ctx);
calc.Init();
var r = await calc.AddAsync(new AddRequest { A = 20, B = 22 });
var counted = new List<int>();
await foreach (var n in calc.Count(new CountRequest { To = 3 })) counted.Add(n.N);
Console.WriteLine($"RESULT 20 + 22 = {r.Result} big={CustomTypes.ToBigInteger(r.Big)} count=[{string.Join(",", counted)}]");

class Calc : ServiceProtobus.Base
{
    public Calc(Context c) : base(c) { }
    public override Task<AddResponse> Add(AddRequest r, CallContext ctx) =>
        Task.FromResult(new AddResponse { Result = r.A + r.B, Big = CustomTypes.Bigint(System.Numerics.BigInteger.Pow(2, 100)) });
    public override async IAsyncEnumerable<Number> Count(CountRequest r, CallContext ctx)
    {
        for (var i = 1; i <= r.To; i++) { await Task.Yield(); yield return new Number { N = i }; }
    }
}
