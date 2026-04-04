# llmrx-core — Execution Model

## Real-world execution trace

A trading firm pipeline: donna (router) → jessica (analyst) → report (writer).
Jessica calls an LLM, uses tools, hits a budget limit, gets human approval via Telegram.

```
                              Oracle Config (cold start)
┌─────────────────────────────────────────────────────────────────────────────┐
│ "llm"                    calls: 1000/day                                    │
│ "llm:anthropic:opus"     cost: $10/day (mutable by uri via telegram)        │
│                          context: 200k tokens, timeout: 45s, retries: 2     │
│ "tool"                   calls: 200/invocation                              │
│ "tool:expensive"         calls: 5/hour                                      │
│ "api:market:alpaca"      calls: 200/min, schedule: CET 9-17                 │
│ "oracle:human:uri:tg"    timeout: 300s                                      │
│                                                                             │
│ global policies:  maxCycles(0), maxDepth(10)                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
exec$(manifest, "donna", "analyze AAPL for trading")
│
├── PolicyState.resetInvocationWindow(invocationId)
│   ┊ only THIS invocation's counters → 0 (concurrent invocations isolated)
│
▼
═══════════════════════════════════════════════════════════════════════════════
executeGraph$()   graphId=g1   ancestry=[donna]   graphIds=[g1]
│
├── PolicyState.resetGraphWindow(g1)
├── classifyEdges() → donna→jessica→report (acyclic DAG, Tarjan SCC)
├── graph:before { key: "donna", nodeCount: 3 }
│
│  ┌─────────────────────────────────────────────────────────────────────────┐
│  │ executeNode$()  donna                                                   │
│  │                                                                         │
│  │  node:before                                                            │
│  │  loadPrompt("analyze AAPL", ctx)                                        │
│  │  │  ctx.counters("llm:anthropic:opus")                                  │
│  │  │  → [{ type: "cost", accumulated: 0, max: 10 },                      │
│  │  │     { type: "context", accumulated: 0, max: 200000 },               │
│  │  │     { type: "calls", accumulated: 0, max: 1000 }]                   │
│  │  │                                                                      │
│  │  execute(layers, ctx)                                                   │
│  │  │                                                                      │
│  │  │  ctx.oracle.llm({                                                    │
│  │  │    oracle: "llm:anthropic:opus",                                     │
│  │  │    call: anthropic.messages.create,                                  │
│  │  │    executeTool: toolRegistry.execute,                                │
│  │  │  })                                                                  │
│  │  │  │                                                                   │
│  │  │  │  ┌─ LLM LOOP (round 0) ─────────────────────────────────────┐    │
│  │  │  │  │                                                           │    │
│  │  │  │  │  execAction$("llm:anthropic:opus", call)                  │    │
│  │  │  │  │  │                                                        │    │
│  │  │  │  │  │  1. CAPTURE (top-down): global → graph → node → llm    │    │
│  │  │  │  │  │     → llm:anthropic → llm:anthropic:opus               │    │
│  │  │  │  │  │     Only force policies. None fire.                    │    │
│  │  │  │  │  │                                                        │    │
│  │  │  │  │  │  2. BUBBLE (bottom-up): llm:anthropic:opus             │    │
│  │  │  │  │  │     → llm:anthropic → llm → node → graph → global     │    │
│  │  │  │  │  │     First deny/stop wins. None fire.                   │    │
│  │  │  │  │  │                                                        │    │
│  │  │  │  │  │  3. PRE-EXEC LIMIT CHECK                              │    │
│  │  │  │  │  │     cost: 0 < 10 ✓                                    │    │
│  │  │  │  │  │     calls: 0 < 1000 ✓                                 │    │
│  │  │  │  │  │     (graph-scoped: checks ALL graphIds in chain)       │    │
│  │  │  │  │  │                                                        │    │
│  │  │  │  │  │  oracle:exec:in { oracle: "llm:anthropic:opus" }       │    │
│  │  │  │  │  │                                                        │    │
│  │  │  │  │  │  4. EXECUTE (with timeout 45s, retries 2)              │    │
│  │  │  │  │  │     → anthropic.messages.create(layers)                │    │
│  │  │  │  │  │     → response: "route to jessica", toolCalls: []      │    │
│  │  │  │  │  │                                                        │    │
│  │  │  │  │  │  5. POST-EXEC ACCUMULATE                              │    │
│  │  │  │  │  │     cost.extract(result) → { prompt: 0.02, out: 0.01 }│    │
│  │  │  │  │  │     accumulated: 0 + 0.03 = 0.03                      │    │
│  │  │  │  │  │     Ticks "llm:anthropic:opus" AND "llm:anthropic"    │    │
│  │  │  │  │  │       AND "llm" counters (oracle hierarchy)            │    │
│  │  │  │  │  │     graph-scoped: ticks ALL graphIds in chain          │    │
│  │  │  │  │  │                                                        │    │
│  │  │  │  │  │  oracle:policy:llm:anthropic:opus { cost: 0.03/10 }   │    │
│  │  │  │  │  │  oracle:exec:out { durationMs: 1200 }                 │    │
│  │  │  │  │  │                                                        │    │
│  │  │  │  │  │  No tool calls → LLM loop done                        │    │
│  │  │  │  │  └───────────────────────────────────────────────────────┘    │
│  │  │  │                                                                   │
│  │  │  → { output: "route to jessica" }                                    │
│  │  │                                                                      │
│  │  node:after { output: "route to jessica" }                              │
│  │  nodeState.set("donna", { layers, output })                             │
│  └─────────────────────────────────────────────────────────────────────────┘
│
│  donna completes → ReplaySubject.next(output) → jessica's forkJoin unblocks
│
│  ┌─────────────────────────────────────────────────────────────────────────┐
│  │ executeNode$()  jessica                                                 │
│  │                                                                         │
│  │  node:before                                                            │
│  │  loadPrompt(["route to jessica"], ctx)                                  │
│  │  │  ctx.node("donna") → { layers, output }  (predecessor access)       │
│  │  │                                                                      │
│  │  execute(layers, ctx)                                                   │
│  │  │                                                                      │
│  │  │  ctx.oracle.llm({                                                    │
│  │  │    oracle: "llm:anthropic:opus",                                     │
│  │  │    call: anthropic.messages.create,                                  │
│  │  │    executeTool: toolRegistry.execute,                                │
│  │  │  })                                                                  │
│  │  │  │                                                                   │
│  │  │  │  ┌─ LLM LOOP (round 0) ─────────────────────────────────────┐    │
│  │  │  │  │                                                           │    │
│  │  │  │  │  execAction$("llm:anthropic:opus", call)                  │    │
│  │  │  │  │  │  cost: 0.03 < 10 ✓   calls: 1 < 1000 ✓               │    │
│  │  │  │  │  │  → response: "analyze", toolCalls: [get_quote(AAPL)]  │    │
│  │  │  │  │  │  accumulated cost: 0.03 + 0.05 = 0.08                 │    │
│  │  │  │  │  │                                                        │    │
│  │  │  │  │  │  execAction$("tool", get_quote)                        │    │
│  │  │  │  │  │  │  tool calls: 0 < 200 ✓                             │    │
│  │  │  │  │  │  │  → { content: "AAPL: $185.50" }                    │    │
│  │  │  │  │  │                                                        │    │
│  │  │  │  │  │  round limits: accumulate("rounds", 1)                 │    │
│  │  │  │  │  └───────────────────────────────────────────────────────┘    │
│  │  │  │  │                                                               │
│  │  │  │  ┌─ LLM LOOP (round 1) ─────────────────────────────────────┐    │
│  │  │  │  │                                                           │    │
│  │  │  │  │  execAction$("llm:anthropic:opus", call)                  │    │
│  │  │  │  │  │  cost: 0.08 < 10 ✓                                    │    │
│  │  │  │  │  │  → response: "AAPL bullish", toolCalls: []            │    │
│  │  │  │  │  │  accumulated cost: 0.08 + 0.04 = 0.12                 │    │
│  │  │  │  │  │  No tool calls → done                                  │    │
│  │  │  │  │  └───────────────────────────────────────────────────────┘    │
│  │  │  │                                                                   │
│  │  │  → { output: "AAPL bullish — buy signal" }                          │
│  │  │                                                                      │
│  │  node:after { output: "AAPL bullish — buy signal" }                     │
│  └─────────────────────────────────────────────────────────────────────────┘
│
│  jessica completes → report's forkJoin unblocks
│
│  ┌─────────────────────────────────────────────────────────────────────────┐
│  │ executeNode$()  report  (same pattern, omitted for brevity)             │
│  │  node:before → loadPrompt → execute → node:after                        │
│  └─────────────────────────────────────────────────────────────────────────┘
│
├── graph:after { key: "donna" }
│
═══════════════════════════════════════════════════════════════════════════════
```

## Nested graph — budget cascade

When donna spawns jessica as a nested graph via `spawnSync`, the inner graph
inherits the full `graphIds` chain. Graph-scoped limits cascade:

```
exec$(manifest, "donna", ...)        graphIds=[g1]
│
├── executeGraph$(donna)              graphIds=[g1]
│   │
│   ├── donna runs, calls spawnSync("jessica")
│   │
│   └── executeGraph$(jessica)        graphIds=[g1, g2]   ← INHERITS g1
│       │
│       ├── jessica runs, calls spawnSync("mike")
│       │
│       └── executeGraph$(mike)       graphIds=[g1, g2, g3]   ← INHERITS g1, g2
│           │
│           └── mike calls exec$("llm:anthropic:opus", ...)
│               │
│               PRE-EXEC CHECK (graph-scoped):
│                 g1 counter: 45/50 ✓
│                 g2 counter: 8/10  ✓
│                 g3 counter: 2/5   ✓
│
│               POST-EXEC ACCUMULATE (graph-scoped):
│                 g1 counter: 45 + 1 = 46
│                 g2 counter: 8 + 1 = 9
│                 g3 counter: 2 + 1 = 3
│                 (ALL ancestor graphs' budgets ticked)
```

If g1 hits 50/50, ALL nested graphs are blocked — no escaping the budget
by spawning deeper.

## Interrupt — mutable limit flow

```
jessica's LLM call:
  execAction$("llm:anthropic:opus", ...)
  │
  PRE-EXEC CHECK:
    cost: 9.80 >= 10.00 → EXCEEDED
    mutable? yes — "oracle:human:uri:tg" is authorized
  │
  oracle:exceeded { oracle: "llm:anthropic:opus", limitType: "cost",
                    accumulated: 9.80, max: 10, mutable: true }
  │
  throw Interrupt({
    type: "limit_exceeded",
    oracleType: "llm:anthropic:opus",
    limitType: "cost",
    window: "day",
    accumulated: 9.80,
    max: 10,
    mutable: { "oracle:human:uri:tg": (next, cur, max) => next <= 50 }
  })
  │
  ▼
executeNode$ catches Interrupt
  │
  resolveInterrupt$(interrupt, "jessica", ctx)
  │
  oracle:interrupt:in { payload: { type: "limit_exceeded", ... } }
  │
  handleInterrupt(interrupt, { spawnSync, node: "jessica", ancestry })
  │  Consumer's handler — e.g. sends Telegram to uri, waits for approval
  │  uri replies: "approve, raise to $25"
  │
  → { decision: "mutate", oracle: "oracle:human:uri:tg", limitType: "cost", max: 25 }
  │
  VALIDATE MUTATION:
    mutableConfig["oracle:human:uri:tg"] exists? yes
    ceiling function: 25 <= 50? yes ✓
  │
  PolicyState.mutateMax("llm:anthropic:opus", "cost", "day", 25)
    previousMax: 10 → newMax: 25
  │
  oracle:mutation:llm:anthropic:opus { limitType: "cost", previousMax: 10,
                                        newMax: 25, approvedBy: "oracle:human:uri:tg" }
  oracle:interrupt:out { decision: "mutate" }
  │
  ▼
executeNode$ retries — re-runs jessica from scratch
  │
  execAction$("llm:anthropic:opus", ...)
    cost: 9.80 < 25 ✓ → proceeds
```

## Concurrent isolation

Two `exec$()` calls on the same `Engine` run with isolated transient counters:

```
Engine (shared PolicyState)
│
├── exec$(manifest, "donna", "task A")    invocationId=inv_1, graphIds=[g1]
│     counter key: "llm:cost:invocation:inv_1"
│     counter key: "llm:cost:graph:g1"
│
├── exec$(manifest, "donna", "task B")    invocationId=inv_2, graphIds=[g2]
│     counter key: "llm:cost:invocation:inv_2"     ← ISOLATED
│     counter key: "llm:cost:graph:g2"              ← ISOLATED
│
│  Time-windowed counters (day, hour, minute) are SHARED:
│     counter key: "llm:cost:day"                   ← SHARED across both
│
│  inv_2's resetInvocationWindow(inv_2) does NOT touch inv_1's counters
```

## Policy resolution — two-phase with hierarchy

```
exec$("llm:anthropic:opus", ...)

Policies collected in bubble order:
  [llm:anthropic:opus policies]     ← most specific
  [llm:anthropic policies]
  [llm policies]                    ← least specific oracle
  [node policies]                   ← manifest.getNodePolicies(nodeKey)
  [graph policies]                  ← manifest.getGraphPolicies(rootAgent)
  [global policies]                 ← constraints.maxCycles, maxDepth, etc.

CAPTURE PHASE (reversed — global first):
  global → graph → node → llm → llm:anthropic → llm:anthropic:opus
  Only policies with { force: true }. First force wins absolutely.

BUBBLE PHASE (original order — specific first):
  llm:anthropic:opus → llm:anthropic → llm → node → graph → global
  First deny/stop wins. Allow continues bubbling.
```

## Signal stream

Every side effect is a typed `Signal` on the observable returned by `exec$()`.
The consumer subscribes and decides what to do — log, store, stream, alert.

```
engine.exec$(manifest, "donna", "go").subscribe(signal => {
  switch (signal.type) {
    case "graph:before":    // { key, nodeCount }
    case "graph:after":     // { key }
    case "graph:error":     // { key, error }
    case "node:before":     // {}
    case "node:after":      // { output }
    case "node:error":      // { error }
    case "oracle:exec:in":  // { oracle }
    case "oracle:exec:out": // { oracle, durationMs }
    case "oracle:denied":   // { oracle, reason }
    case "oracle:exceeded": // { oracle, limitType, accumulated, max, mutable }
    case "oracle:interrupt:in":  // { payload }
    case "oracle:interrupt:out": // { payload, decision }
  }
  // All signals carry: nodeKey, graphId, ancestry, invocationId,
  //                     nodeId, oracleId, policyId, timestamp
});
```

## Persistence

```typescript
// shutdown — snapshot all counters
const snap = engine.snapshot();
await db.save("llmrx_state", snap);

// boot — restore counters (time-windowed limits survive restarts)
const saved = await db.load("llmrx_state");
if (saved) engine.restore(saved);
```

## Abort

```typescript
const controller = new AbortController();
engine.exec$(manifest, "donna", "go", controller.signal).subscribe(...);

// later
controller.abort();
// All pending exec$ calls throw Aborted
// ReplaySubjects complete cleanly — no forkJoin deadlocks
```
