using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Combat;
using Protobus;

namespace Examples;

/// <summary>
/// A battle royale played over protobus: six players, each its own instance of the Combat.Player
/// service (Combat.Player.player1 ... player6) with its own strategy. Players shoot each other by
/// RPC; joins, hits, deaths, turns and the winner travel as events every player subscribes to.
///
/// A port of the TypeScript, Python, Go, C++ and Java combat samples, on the same schema: it plays
/// against their players unchanged.
/// </summary>
public static class CombatExample
{
    private const int StartingHealth = 10;
    private static readonly object Console_ = new();

    private static void Say(string line)
    {
        lock (Console_) Console.WriteLine(line);
    }

    private sealed class Opponent
    {
        public readonly string Id;
        public readonly string Name;
        public int Health = StartingHealth;
        public bool Alive = true;

        public Opponent(string id, string name)
        {
            Id = id;
            Name = name;
        }
    }

    /// <summary>What a strategy may read and keep.</summary>
    private sealed class View
    {
        public int Health = StartingHealth;
        public string LastAttacker = "";
        public string Focus = ""; // strategies that hold a grudge store it here
        public readonly Random Rand;

        public View(int seed) => Rand = new Random(seed);
    }

    private static Opponent? RandomOf(View v, List<Opponent> alive) => alive.Count == 0 ? null : alive[v.Rand.Next(alive.Count)];

    /// <summary>The six strategies of the original sample.</summary>
    private static readonly (string Name, Func<View, List<Opponent>, Opponent?> Pick)[] Strategies =
    {
        ("The Vindicator", (v, alive) => alive.FirstOrDefault(o => o.Id == v.LastAttacker) ?? RandomOf(v, alive)),
        ("The Bully Hunter", (_, alive) => alive.MinBy(o => o.Health)),
        ("The Giant Slayer", (_, alive) => alive.MaxBy(o => o.Health)),
        ("The Equalizer", (v, alive) => alive.MinBy(o => Math.Abs(o.Health - v.Health))),
        ("The Wildcard", RandomOf),
        ("The Terminator", (v, alive) =>
        {
            var o = alive.FirstOrDefault(x => x.Id == v.Focus) ?? RandomOf(v, alive);
            if (o != null) v.Focus = o.Id;
            return o;
        }),
    };

    /// <summary>
    /// One contestant: an instance of Combat.Player. Requests and events are separate consumers,
    /// so handlers run concurrently and the state is guarded.
    /// </summary>
    private sealed class Player : PlayerProtobus.Base
    {
        public readonly string Id;
        public readonly string Name;
        private readonly Func<View, List<Opponent>, Opponent?> strategy;
        private readonly View view;
        private readonly object sync = new();
        private readonly Dictionary<string, Opponent> others = new();
        private List<string> order = new();
        private bool gameOver;

        public Player(Context ctx, string id, string name, Func<View, List<Opponent>, Opponent?> strategy, int seed) : base(ctx)
        {
            Id = id;
            Name = name;
            this.strategy = strategy;
            view = new View(seed);
        }

        /// <summary>Each player is an instance of the one Combat.Player contract.</summary>
        public override string ServiceName => "Combat.Player." + Id;

        public override async Task<ShootResponse> Shoot(ShootRequest request, CallContext context)
        {
            bool hit;
            int health;
            var shooter = request.ShooterId;
            lock (sync)
            {
                if (view.Health <= 0) return new ShootResponse { Hit = false };
                hit = view.Rand.Next(2) == 1;
                if (hit)
                {
                    view.Health--;
                    view.LastAttacker = request.ShooterId;
                }
                health = view.Health;
                if (others.TryGetValue(shooter, out var o)) shooter = o.Name;
            }
            Say(hit ? $"  {Name} was hit by {shooter}! Health: {health}" : $"  {Name} dodged an attack from {shooter}!");
            await PublishEventAsync(new PlayerShot { ShooterId = request.ShooterId, TargetId = Id, Hit = hit, TargetHealth = health });
            if (hit && health <= 0)
            {
                Say($"  {Name} has been eliminated!");
                await PublishEventAsync(new PlayerDied { PlayerId = Id, KilledBy = request.ShooterId });
            }
            return new ShootResponse { Hit = hit, RemainingHealth = health };
        }

        /// <summary>The turn order. The first player takes its turn as a task of its own, so the RPC answers at once.</summary>
        public override Task<InitiateGameResponse> InitiateGame(InitiateGameRequest request, CallContext context)
        {
            lock (sync) order = request.PlayerOrder.ToList();
            if (request.MyIndex == 0) _ = Task.Run(TakeTurnAsync);
            return Task.FromResult(new InitiateGameResponse { Success = true });
        }

        public override Task<GetStatusResponse> GetStatus(GetStatusRequest request, CallContext context)
        {
            lock (sync)
                return Task.FromResult(new GetStatusResponse { PlayerId = Id, PlayerName = Name, Health = view.Health, Alive = view.Health > 0 });
        }

        public void Meet(string otherId, string otherName)
        {
            lock (sync)
                if (otherId != Id) others.TryAdd(otherId, new Opponent(otherId, otherName));
        }

        public async Task SubscribeAsync()
        {
            await SubscribeEventAsync<PlayerJoined>((e, _, _) =>
            {
                Meet(e.PlayerId, e.PlayerName);
                return Task.CompletedTask;
            });
            await SubscribeEventAsync<PlayerShot>((e, _, _) =>
            {
                lock (sync)
                {
                    if (others.TryGetValue(e.TargetId, out var o))
                    {
                        o.Health = e.TargetHealth;
                        o.Alive = e.TargetHealth > 0;
                    }
                }
                return Task.CompletedTask;
            });
            await SubscribeEventAsync<PlayerDied>((e, _, _) =>
            {
                lock (sync)
                {
                    if (others.TryGetValue(e.PlayerId, out var o))
                    {
                        o.Health = 0;
                        o.Alive = false;
                    }
                    if (view.Focus == e.PlayerId) view.Focus = "";
                }
                return Task.CompletedTask;
            });
            await SubscribeEventAsync<TurnComplete>(async (e, _, _) =>
            {
                bool mine;
                lock (sync) mine = order.IndexOf(Id) == e.NextPlayerIndex;
                if (mine) await TakeTurnAsync();
            });
            await SubscribeEventAsync<GameOver>((_, _, _) =>
            {
                lock (sync) gameOver = true;
                return Task.CompletedTask;
            });
        }

        private List<Opponent> AliveOthers() => others.Values.Where(o => o.Alive).ToList();

        private async Task TakeTurnAsync()
        {
            Opponent? target = null;
            lock (sync)
            {
                if (gameOver) return;
                if (view.Health > 0)
                {
                    var alive = AliveOthers();
                    if (alive.Count == 0) goto win;
                    target = strategy(view, alive);
                }
            }
            // A turn handed to a player who died meanwhile is passed on, not dropped: dropping it
            // would stall the game.
            if (target != null)
            {
                Say($"  {Name} shoots at {target.Name}!");
                var victim = new PlayerProtobus.Proxy(Context, "Combat.Player." + target.Id);
                victim.Init();
                try
                {
                    var result = await victim.ShootAsync(new ShootRequest { ShooterId = Id }, new CallOptions { Actor = Id });
                    if (result.RemainingHealth <= 0)
                    {
                        lock (sync)
                        {
                            target.Alive = false;
                            target.Health = 0;
                        }
                    }
                }
                catch (ProtobusException e)
                {
                    Say($"  {Name} failed to shoot: {e.Message}");
                }
                bool none;
                lock (sync) none = AliveOthers().Count == 0;
                if (none) goto win;
            }
            await EndTurnAsync();
            return;
            win:
            Say($"  {Name} is the last one standing!");
            await PublishEventAsync(new GameOver { WinnerId = Id, WinnerName = Name });
        }

        private Task EndTurnAsync()
        {
            int next;
            lock (sync)
            {
                var me = order.IndexOf(Id);
                var n = order.Count;
                next = (me + 1) % n;
                for (var i = 1; i <= n; i++)
                {
                    var idx = (me + i) % n;
                    if (others.TryGetValue(order[idx], out var o) && o.Alive)
                    {
                        next = idx;
                        break;
                    }
                }
            }
            return PublishEventAsync(new TurnComplete { PlayerId = Id, NextPlayerIndex = next });
        }
    }

    public static async Task<int> RunAsync()
    {
        // The game narrates itself; the framework's own lines would drown it out.
        Logger.Level = LogLevel.Warn;
        await using var context = new Context();
        await context.InitAsync(Program.AmqpUrl);
        var rule = new string('=', 60);
        Say($"{rule}\nCOMBAT GAME - Battle Royale!\n{rule}");

        // Hear the result like any other subscriber.
        var results = new EventListener(context.Connection);
        await results.InitAsync(null, "");
        var winner = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        await results.SubscribeAsync<GameOver>((e, _, _) =>
        {
            winner.TrySetResult(e.WinnerName);
            return Task.CompletedTask;
        });
        await results.StartAsync();

        var seed = Environment.TickCount;
        var players = new List<Player>();
        var order = new List<string>();
        for (var i = 0; i < Strategies.Length; i++)
        {
            var id = "player" + (i + 1);
            var p = new Player(context, id, Strategies[i].Name, Strategies[i].Pick, seed + i);
            await p.InitAsync();
            await p.SubscribeAsync();
            players.Add(p);
            order.Add(id);
            Say($"  joined: {p.Name} ({id})");
        }
        foreach (var p in players)
        {
            foreach (var o in players) p.Meet(o.Id, o.Name);
            await context.PublishEventAsync(new PlayerJoined { PlayerId = p.Id, PlayerName = p.Name, Health = StartingHealth });
        }
        Say($"Turn order: {string.Join(" -> ", order)}\n{rule}\nLET THE BATTLE BEGIN!\n{rule}");

        // Index 0 is initiated last: its first turn passes the turn on, and every other player must
        // know the order by then.
        for (var idx = order.Count - 1; idx >= 0; idx--)
        {
            var player = new PlayerProtobus.Proxy(context, "Combat.Player." + order[idx]);
            player.Init();
            var request = new InitiateGameRequest { MyIndex = idx };
            request.PlayerOrder.Add(order);
            await player.InitiateGameAsync(request);
        }

        try
        {
            await winner.Task.WaitAsync(TimeSpan.FromMinutes(2));
        }
        catch (TimeoutException)
        {
            Console.Error.WriteLine("the game did not finish");
            return 1;
        }

        Say($"{rule}\nFINAL RESULTS\n{rule}");
        foreach (var id in order)
        {
            var player = new PlayerProtobus.Proxy(context, "Combat.Player." + id);
            player.Init();
            var status = await player.GetStatusAsync(new GetStatusRequest());
            Say($"  {status.PlayerName,-18}{status.Health,2} HP  ({(status.Alive ? "WINNER" : "eliminated")})");
        }
        foreach (var p in players) await p.StopConsumingAsync();
        await context.Connection.DrainInFlightAsync(5000);
        return 0;
    }
}
