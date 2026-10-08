# Cross-language suite

protobus-csharp against the TypeScript, Python, Go, C++ and Java ports' real
libraries, over a real broker, in both directions:

| Test | What runs |
|---|---|
| `TheCSharpClientAgainstEveryServer({csharp,ts,py,go,cpp,java})` | the C# client scenario against each language's server |
| `EveryClientAgainstACSharpServer({ts,py,go,cpp,java})` | each language's client scenario against the C# server |
| `MixedReplicasShareOneRetryLadder` | `interop.Flaky` served in all six languages at once, sharing one queue and one retry ladder |

Every client runs the same fifteen checks (unary calls, priority, streams in
order, empty streams, mid-stream errors, cancellation reaching the producer,
custom types with maps and defaults, an echo round trip, handled, unhandled and
protocol errors, call metadata, instance routing, and events both ways). The
mixed-replica test requires every message to be attempted exactly four times,
in more than one language, and to reach `interop.Flaky.DLQ` exactly once with
the shared metadata headers. Each test runs in a fresh vhost.

## Layout

- `proto/interop.proto`: the contract, identical to the other ports'.
- `CSharpPeer/`: the C# participant (`server` / `client`).
- `peers/ts/peer.js`, `peers/py/peer.py`, `peers/go/main.go`: the TypeScript,
  Python and Go participants, the same files protobus-java's suite runs.
- The C++ participant is protobus-cpp's own `cpppeer`, from its build tree;
  the Java participant is protobus-java's `JavaPeer`, from its `crosslang`
  module.
- `Protobus.CrossLang.Tests/`: the harness.

## Running it

```bash
docker compose up -d --wait
export PROTOBUS_TEST_AMQP_URL=amqp://guest:guest@127.0.0.1:25672/
export PROTOBUS_TEST_MGMT_URL=http://guest:guest@127.0.0.1:25673
(cd ../protobus && npm ci && npm run build-ts)        # the TypeScript peer runs the built library
(cmake -S ../protobus-cpp -B ../protobus-cpp/build && cmake --build ../protobus-cpp/build --target cpppeer)
export JAVA_HOME=...                                  # a JDK 17+, for the Java peer
dotnet test crosslang/Protobus.CrossLang.Tests
```

The peers are found in sibling checkouts: `PROTOBUS_TS` (default
`../protobus`), `PROTOBUS_PY` (default `../protobus-py`, with a `venv/`),
`PROTOBUS_GO` (default `../protobus-go`, with Go on the PATH), `PROTOBUS_CPP`
(default `../protobus-cpp`, with `build/crosslang/cpppeer`) and
`PROTOBUS_JAVA` (default `../protobus-java`, with `JAVA_HOME` set). A missing
peer skips its tests, unless its variable is set explicitly, as CI does: then
its absence is a failure, so a misconfigured run cannot pass by skipping.

Any peer can be run by hand against any server:

```bash
PROTOBUS_TEST_AMQP=$PROTOBUS_TEST_AMQP_URL PEER_TARGET=csharp node crosslang/peers/ts/peer.js client
```
