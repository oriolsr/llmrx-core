# llmrx-core

[![npm version](https://img.shields.io/npm/v/llmrx-core.svg)](https://www.npmjs.com/package/llmrx-core)
[![license](https://img.shields.io/npm/l/llmrx-core.svg)](./LICENSE)

Reactive graph execution engine for LLM agents. DAG, DG, or hybrid. Single file. One dependency (`rxjs`).

Every action — LLM call, tool execution, API request, human approval — flows through `exec$` with two-phase policy resolution, limit tracking, and interrupt handling.

> **⚠ API unstable** — breaking changes possible until v1.0. Pin to an exact version.

## Install

```bash
pnpm add llmrx-core
```

Runnable examples live in [`examples/`](./examples):
- [`minimal-agent.ts`](./examples/minimal-agent.ts) — a single-node agent with one tool, one oracle, one signal stream
- [`policy-budget-interrupt.ts`](./examples/policy-budget-interrupt.ts) — cost limit → mutable policy → interrupt → approval → mutate and resume

## What it does

- **DAG/DG/Hybrid scheduling** — topological ordering, parallel branches, back-edge detection, round-based cycle iteration
- **Oracle types** — every async action goes through a registered oracle with its own policies. Hierarchical keys: `"llm:anthropic:claude"` inherits from `"llm:anthropic"` and `"llm"`
- **Two-phase policy resolution** — capture (top-down, force only) then bubble (bottom-up, first deny/stop wins)
- **Built-in limit tracking** — cost, calls, context, rounds, timeout, retries. Time-windowed counters per `(oracle, type, window)`
- **Mutable policies** — declare which oracles may raise a limit's `max` at runtime, with ceiling functions. Auto-interrupts instead of killing
- **Nested graphs** — `spawnSync` (blocking), `spawnAsync` (fire-and-forget)
- **Full observability** — every exec$, policy check, mutation, and lifecycle event is a typed `Signal`

## What it does NOT do

- Choose your LLM provider — you inject `call`
- Define your tools — you inject `executeTool`
- Structure your prompts — you implement `loadPrompt`
- Pick your database — your handlers do IO
- Decide what permissions mean — your policies resolve them
- Choose your approval flow — you implement `handleInterrupt`
- Log anything — subscribe to the signal stream

## Quick start

```typescript
import { createEngine, constraints } from "llmrx-core";

const engine = createEngine({
  oracles: {
    "llm": {
      policies: [
        { type: "calls", max: 1000, window: "day" },
      ],
    },
    "llm:anthropic:claude": {
      policies: [
        {
          type: "cost", max: 10, window: "day",
          extract: (r) => {
            const u = (r as { usage: { input_cost: number; output_cost: number } }).usage;
            return { prompt: u.input_cost, completion: u.output_cost };
          },
          mutable: {
            "oracle:human:uri:telegram": (next, current, max) => next <= 50,
            "oracle:human:cfo:email": true,
          },
        },
        { type: "timeout", max: 45_000 },
        { type: "retries", max: 2 },
      ],
    },
    "tool": {
      policies: [
        { type: "calls", max: 200, window: "invocation" },
      ],
    },
    "api:market:alpaca": {
      policies: [
        { type: "calls", max: 200, window: "minute" },
        { policy: (action) => {
            const h = new Date(action.timestamp)
              .toLocaleString("en", { timeZone: "CET", hour: "numeric", hour12: false });
            return +h >= 9 && +h < 17
              ? { approval: "allow" as const }
              : { approval: "deny" as const, reason: "outside CET trading hours" };
          },
        },
      ],
    },
  },
  policies: [constraints.maxCycles(0), constraints.maxDepth(10)],
  handleInterrupt: async (interrupt, ctx) => {
    // route to approval oracle, return retry/deny/result/mutate
  },
});

engine.exec$(manifest, "router", "user message")
  .subscribe((event) => {
    // typed union — switch on event.type
  });
```

## Oracle API

Inside `Node.execute()`, the consumer gets `ctx.oracle`:

### oracle.llm() — the LLM loop

Call LLM, execute tools, loop. Round limits come from oracle policies.

```typescript
execute(layers, ctx) {
  return ctx.oracle.llm({
    oracle: "llm:anthropic:claude",
    // turns: append-only transcript of assistant + tool_result turns,
    // grown by the engine across rounds. Your provider translates it
    // to its wire format (Anthropic blocks, OpenAI tool_call_id, etc.)
    // and pairs each tool_result with its tool_use_id.
    call: async (turns) => {
      const r = await anthropic.messages.create({ system: layers, messages: toMessages(turns) });
      const text = r.content.find(b => b.type === "text")?.text ?? "";
      const tool_calls = r.content.filter(b => b.type === "tool_use")
        .map(b => ({ id: b.id, name: b.name, input: b.input }));
      return { text, tool_calls };
    },
    executeTool: (tc, ctx) => toolRegistry.execute(tc, ctx), // tc: { id, name, input }
  });
}
```

### oracle.call() — generic tracked call

Any async work. Tracked, policy-checked, timed.

```typescript
ctx.oracle.call("api:market:alpaca", () => fetchPrices("AAPL"))
ctx.oracle.call("oracle:human:uri:telegram", () => sendAndWait(trade))
```

### exec$

Two forms — registered oracle or ad-hoc:

```typescript
// Registered oracle — engine calls its executor
ctx.exec$("api:market", { symbol: "AAPL" })

// Ad-hoc — you provide the function
ctx.exec$("llm:anthropic:claude", () => callProvider(layers))
```

## Policies

Two kinds on one array:

### Built-in policies (OraclePolicy)

Engine handles counting, windows, and extraction:

| type | auto-counted | needs extract | description |
|------|-------------|---------------|-------------|
| `cost` | | yes | granular breakdown: `{ prompt: 0.02, completion: 0.01 }` |
| `calls` | +1 per exec$ | | |
| `context` | | yes | e.g. `{ input_tokens: 3200, output_tokens: 1000 }` |
| `rounds` | +1 per llm loop | | only inside oracle.llm() |
| `retries` | | | engine wraps exec$ with retry(max). optional `backoff` |
| `timeout` | | | engine wraps exec$ with timeout(max) ms |
| `x:*` | | yes | custom. engine just tracks the counter |

```typescript
interface OraclePolicy {
  type: string;
  max: number;
  window?: TimeWindow;
  extract?: (result: unknown) => Record<string, number>;
  mutable?: Record<string, true | ((next: number, current: number, max: number) => boolean)>;
  backoff?: "exponential" | number | ((attempt: number) => number);
}
```

Backoff applies to `type: "retries"`:
- `"exponential"` — 1s, 2s, 4s, 8s... (`1000 * 2^attempt`)
- `number` — fixed ms delay between retries
- `(attempt) => number` — custom function, receives 0-indexed attempt

### Custom policies (CustomPolicy)

Consumer implements `policy()`:

```typescript
interface CustomPolicy {
  id?: string;
  policy(action: PolicyAction): PolicyResult;
}
```

### PolicyResult — what policies return

```
{ approval: "allow" }                              continue bubbling
{ approval: "allow", stop: true }                  stop bubbling here
{ approval: "allow", force: true }                 capture phase — override everything
{ approval: "deny", reason: "..." }                deny, stop
{ approval: "deny", reason: "...", force: true }   capture phase — force deny
{ approval: "interrupt", interrupt: ... }          pause execution
```

## Two-phase resolution

Every `exec$` resolves policies in bubble order:

```
exec$("llm:anthropic:claude", ...)

CAPTURE (top-down):  global → graph → node → "llm" → "llm:anthropic" → "llm:anthropic:claude"
  Only force policies. If any fires, that's final.

BUBBLE (bottom-up):  "llm:anthropic:claude" → "llm:anthropic" → "llm" → node → graph → global
  Normal policies. First deny/stop wins.
```

## Mutable policies

Any limit can declare who may raise its `max` at runtime:

```typescript
{
  type: "cost", max: 10, window: "day",
  extract: (r) => ({ prompt: r.usage.input_cost, completion: r.usage.output_cost }),
  mutable: {
    "oracle:human:uri:telegram": (next, current, max) => next <= 50,  // ceiling function
    "oracle:human:cfo:email": true,                                   // approve anything
  },
}
```

When exceeded: auto-interrupt → `handleInterrupt` → approver returns `{ decision: "mutate", oracle, limitType, max }` → engine validates ceiling → mutate + retry.

## Constraints

Shipped as `CustomPolicy` factories. Put them at any level — global, graph, or node.

```typescript
constraints.maxCycles(0)                // no node can repeat in ancestry
constraints.maxDepth(10)                // max spawn depth
constraints.maxLoop("feedback_loop", 3) // allow this node to loop 3x (stops bubbling)
```

## Time windows

| window | resets |
|--------|--------|
| `"minute"` / `"hour"` / `"day"` / `"week"` / `"month"` | sliding window |
| `"graph"` | on each graph execution start |
| `"invocation"` | on each exec$() call |
| `number` | custom ms, sliding |

## Signals

Every side effect is a typed `Signal`. All carry `{ nodeKey, invocationId, graphId, nodeId, ancestry }`.

| type | when |
|------|------|
| `graph:before` / `graph:after` / `graph:error` | graph lifecycle (carry `key`) |
| `node:before` / `node:after` / `node:error` | node lifecycle |
| `oracle:exec:in` / `oracle:exec:out` | exec$ lifecycle |
| `oracle:denied` | custom policy denied |
| `oracle:exceeded` | limit exceeded pre-exec |
| `oracle:interrupt:in` / `oracle:interrupt:out` | interrupt lifecycle |
| `oracle:policy:${key}` | policy limit check post-exec |
| `oracle:mutation:${key}` | policy limit mutated at runtime |
| `x:*` | consumer extension namespace |

## Architecture

```
exec$(manifest, key, message, signal?)
  └─ executeGraph$(topology)     — schedule nodes by edges, handle cycles
       └─ executeNode$(nodeKey)  — load prompt → execute → after
            └─ exec$()           — policies → check → execute → extract → accumulate
```

## Types

```typescript
// ── Manifest — what the consumer provides ──

interface Manifest {
  getNode(key: string): Node;
  getNodeKeys(): string[];
  isNode(key: string): boolean;
  getGraph(key: string): Graph;
  getGraphPolicies?(key: string): Policy[] | null;
  getNodePolicies?(nodeKey: string): Policy[] | null;
  newId?(entity: "manifest" | "graph" | "node" | "oracle" | "policy"): string;
  getMap?(nodeKey: string): MapConfig | null;
}

interface Graph {
  readonly nodes: string[];
  readonly edges: Array<{ from: string; to: string }>;
}

interface MapConfig {
  readonly items: string;         // key in upstream output to iterate
  readonly subTopology: Graph;    // sub-graph executed per item
}

interface Oracle {
  policies: Policy[];
  executor?: (input: unknown) => Promise<unknown>;
}

interface Node {
  loadPrompt(input: unknown, ctx: NodeExec): Observable<Layer[]>;
  execute(layers: Layer[], ctx: NodeExec): Observable<NodeResult>;
}

// ── createEngine → Engine ──

interface EngineDef {
  oracles: Record<string, Oracle>;
  policies?: CustomPolicy[];
  handleInterrupt?: (interrupt: Interrupt, ctx: InterruptCtx) => Promise<InterruptResult>;
  interruptTimeout?: number;      // default 120_000 ms
}

function createEngine(config: EngineDef): Engine;

interface Engine {
  exec$(manifest: Manifest, key: string, input: unknown, signal?: AbortSignal): Observable<Signal>;
  snapshot(): PolicySnapshot;
  restore(snap: PolicySnapshot): void;
}

// ── Errors — thrown by exec$ ──

class Interrupt { constructor(public readonly payload: unknown) {} }
class Denied extends Error { readonly node: string; readonly oracle: string; readonly reason: string; }
class PolicyExceeded extends Error { readonly label: string; readonly value: number; readonly limit: number; }
class Aborted extends Error {}

// ── Runtime — what execute() receives ──

interface NodeExec {
  counters(oracle: string): ReadonlyArray<{ type: string; accumulated: number; max: number; window?: TimeWindow }>;
  exec$<T>(oracle: string, input?: unknown): Observable<T>;
  exec$<T>(oracle: string, fn: () => Promise<T>, input?: unknown): Observable<T>;
  oracle: OracleExec;
  spawnSync(opts: { key: string; data: unknown }): Observable<Signal>;
  spawnAsync(opts: { key: string; data: unknown }): void;
  handleInterrupt(interrupt: Interrupt): Observable<ResolvedInterrupt>;
  checkAncestry(target: string): string | null;
  node(key: string): { layers: Layer[]; output: unknown } | null;
  ancestry: readonly AncestryEntry[];
  keys: string[];
  isNode(key: string): boolean;
  readonly invocationId: string;
  readonly graphId: string;
  readonly nodeId: string;
}

interface ToolCall { id: string; name: string; input: unknown; }
interface ToolResult { tool_use_id: string; content: string; is_error?: boolean; }
type Turn =
  | { role: "user"; content: string }
  | { role: "assistant"; text: string; tool_calls?: ToolCall[] }
  | { role: "tool_result"; results: ToolResult[] };

interface OracleExec {
  llm(opts: {
    oracle: string;
    call: (turns: Turn[]) => Promise<{ text: string; tool_calls?: ToolCall[] }>;
    executeTool?: (tc: ToolCall, ctx: NodeExec) => Promise<{ content: string; is_error?: boolean; signals?: Signal[] }>;
  }): Observable<NodeResult>;
  call<T>(oracle: string, fn: () => Promise<T>, input?: unknown): Observable<T>;
}

interface NodeResult {
  output: unknown;
  signals?: Signal[];
}

interface Layer {
  name: string;
  content: unknown;
}

// ── Policy types ──

interface PolicyAction {
  readonly node: string;
  readonly oracle: string;
  readonly input: unknown;
  readonly history: readonly string[];
  readonly ancestry: readonly AncestryEntry[];
  readonly target?: string;
  readonly timestamp: number;
  readonly invocationId: string;
  readonly graphId: string;
  readonly nodeId: string;
  readonly oracleId: string;
  readonly policyId: string;
}

type InterruptResult =
  | { decision: "retry" }
  | { decision: "deny"; reason: string }
  | { decision: "result"; value: unknown }
  | { decision: "mutate"; oracle: string; limitType: string; max: number }

// ── Primitives ──

type AncestryEntry = {
  readonly nodeKey: string;
  readonly nodeId: string;
  readonly graphId: string;
  readonly oracleId: string;
  readonly policyId: string;
};

type TimeWindow = "minute" | "hour" | "day" | "week" | "graph" | "invocation" | number;

type Policy = OraclePolicy | CustomPolicy;

type PolicySnapshot = ReadonlyArray<{
  key: string;
  accumulated: number;
  max: number;
  originalMax: number;
  windowStart: number;
}>;
```

## Persistence

```typescript
// shutdown
const snap = engine.snapshot();
await db.save("llmrx_state", snap);

// boot
const saved = await db.load("llmrx_state");
if (saved) engine.restore(saved);
```

## Abort

Pass an `AbortSignal` to `exec$()`. Pending `exec$` calls throw `Aborted`.

## License

MIT
