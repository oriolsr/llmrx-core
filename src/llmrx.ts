/**
 * llmrx — Reactive execution engine for autonomous AI systems.
 *
 * Policy-driven oracles. One function. One stream.
 *
 * The graph knows:
 *   - execAction$: wraps every action with two-phase policy resolution + limits
 *   - Oracle: hierarchical, policy-driven, with built-in limit tracking
 *   - Node: definition — prompt loading + execution logic
 *   - Signal: every side effect visible in the stream
 *
 * The graph does NOT know:
 *   - What layers exist (soul, tools, memory — consumer's domain)
 *   - What tools exist (consumer injects executeTool into oracle.llm)
 *   - What DB is used (consumer's domain)
 *   - What LLM is used (consumer injects call into oracle.llm)
 *   - What actions exist (consumer's permission namespace)
 *   - How layers are assembled (consumer's loadPrompt)
 *   - Logging format (subscriber decides)
 */

import {
  Observable, Subject, from, of, merge, defer, EMPTY, concat,
  forkJoin, ReplaySubject, lastValueFrom, timer,
} from "rxjs";
import {
  concatMap, tap, finalize,
  catchError, retry, timeout, first, toArray,
} from "rxjs/operators";

// ═══════════════════════════════════════════════════════════
// INTERRUPT — the universal pause primitive
// ═══════════════════════════════════════════════════════════

/** Throwable by anything inside execAction$. Pauses execution, consumer handles, resumes or denies. */
export class Interrupt {
  constructor(
    /** Opaque — consumer decides what this means (targets, UI type, resolution strategy). */
    public readonly payload: unknown,
  ) {}
}

/** What the consumer returns after handling an interrupt. */
export type InterruptResult =
  | { decision: "retry" }
  | { decision: "deny"; reason: string }
  | { decision: "result"; value: unknown }
  | { decision: "mutate"; oracle: string; limitType: string; max: number };

/** Context provided to the consumer's interrupt handler. */
export interface InterruptCtx {
  /** Run a nested graph (blocking) — same execAction$, same policies, same signals. */
  spawnSync(opts: { key: string; data: unknown }): Observable<Signal>;
  /** Who was executing when the interrupt fired. */
  node: string;
  /** Current ancestry chain. */
  ancestry: readonly AncestryEntry[];
}

// ═══════════════════════════════════════════════════════════
// POLICY — unified permission + limit system
// ═══════════════════════════════════════════════════════════

/** What policies receive — context for every execAction$ call. */
export interface PolicyAction {
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

/** What custom policies return. */
export type PolicyResult =
  | { approval: "allow" }
  | { approval: "allow"; stop: true }
  | { approval: "allow"; force: true }
  | { approval: "deny"; reason: string }
  | { approval: "deny"; reason: string; force: true }
  | { approval: "interrupt"; interrupt: Interrupt };

/** Consumer-defined policy with policy(). */
export interface CustomPolicy {
  id?: string;
  policy(action: PolicyAction): PolicyResult;
}

/** Time window for limit policies. */
export type TimeWindow =
  | "minute" | "hour" | "day" | "week" | "month"
  | "graph" | "invocation"
  | number;

/** Built-in limit policy — engine handles counting, windows, extraction. */
export interface OraclePolicy {
  type: string;
  max: number;
  window?: TimeWindow;
  extract?: (result: unknown) => Record<string, number>;
  mutable?: Record<string, true | ((next: number, current: number, max: number) => boolean)>;
  /** Backoff between retries. Only applies to type: "retries". */
  backoff?: "exponential" | number | ((attempt: number) => number);
}

/** A policy is either a built-in limit or a custom policy function. */
export type Policy = OraclePolicy | CustomPolicy;

/** Type guard: is this policy a CustomPolicy with policy()? */
function isCustomPolicy(p: Policy): p is CustomPolicy {
  return "policy" in p && typeof (p as CustomPolicy).policy === "function";
}

/** Type guard: is this policy a built-in OraclePolicy? */
function isOraclePolicy(p: Policy): p is OraclePolicy {
  return "type" in p && "max" in p && !isCustomPolicy(p);
}

// ═══════════════════════════════════════════════════════════
// ORACLE TYPES — hierarchical, policy-driven
// ═══════════════════════════════════════════════════════════

/** Oracle definition — registered at cold start. Policies + optional executor. */
export interface Oracle {
  policies: Policy[];
  /**
   * If provided, exec$(oracle, input) calls this without needing fn.
   * Must return an Observable — one-shot oracles emit once and complete,
   * stream oracles emit many times until unsubscribed.
   *
   * When the executor is invoked from inside a node (via ctx.exec$), the
   * engine passes the current NodeExec as the second arg so the executor
   * can reach spawnSync/spawnAsync/ancestry. When invoked from a top-level
   * runtime.exec with no node context (e.g. entry-point calls), ctx is
   * undefined. Executors that need a ctx should throw if it's missing.
   */
  executor?: (input: unknown, ctx?: NodeExec) => Observable<unknown>;
}

// ═══════════════════════════════════════════════════════════
// LAYERS & NODE — the core data model
// ═══════════════════════════════════════════════════════════

/** A single prompt fragment flowing through the engine. */
export interface Layer {
  name: string;
  content: unknown;
  /** Provider-specific cache hint (e.g. "ephemeral"). null = no hint. */
  cache?: string | null;
}

/** What the consumer's execute() returns. */
export interface NodeResult {
  output: unknown;
  signals?: Signal[];
}

/** OracleExec — runtime oracle API exposed on NodeExec. */
export interface OracleExec {
  /** LLM loop — call → tools → loop. Returns when no tool calls remain. */
  llm(opts: {
    /** Oracle key for LLM calls (e.g. "llm:anthropic:opus"). Costs accumulate up the hierarchy. */
    oracle: string;
    call: (layers: Layer[], history: unknown[]) => Promise<{ response: string; toolCalls?: unknown[] }>;
    executeTool?: (toolCall: unknown, ctx: NodeExec) => Promise<{ content: string; signals?: Signal[] }>;
  }): Observable<NodeResult>;

  /** Generic tracked call — any oracle, any Observable work. Wrap Promises with from(). */
  call<T>(oracle: string, fn: () => Observable<T>, input?: unknown): Observable<T>;
}

/** Consumer provides via Manifest.getNode() — prompt loader + execution logic. */
export interface Node {
  /** Build prompt layers from input and runtime context. */
  loadPrompt(input: unknown, ctx: NodeExec): Observable<Layer[]>;
  /** Execute the node. Consumer owns the logic — call oracle.llm(), oracle.call(), or raw exec$(). */
  execute(layers: Layer[], ctx: NodeExec): Observable<NodeResult>;
}

/** Context provided to the consumer's Node.execute(). The engine's entire runtime API surface. */
export interface NodeExec {
  /** Query accumulated counters for a given oracle (walks hierarchy). */
  counters(oracle: string): ReadonlyArray<{ type: string; accumulated: number; max: number; window?: TimeWindow }>;
  /** exec$ — wrap any Observable action with policy/limits/interrupts. */
  exec$<T>(oracle: string, input?: unknown): Observable<T>;
  exec$<T>(oracle: string, fn: () => Observable<T>, input?: unknown): Observable<T>;
  /** OracleExec — ergonomic wrappers around exec$. */
  oracle: OracleExec;
  /** Run a nested graph (blocking). */
  spawnSync(opts: { key: string; data: unknown }): Observable<Signal>;
  /** Run a nested graph (fire-and-forget) — runs at node completion. */
  spawnAsync(opts: { key: string; data: unknown }): void;
  /** Handle an interrupt inline. */
  handleInterrupt(interrupt: Interrupt): Observable<ResolvedInterrupt>;
  /** Current ancestry chain. */
  ancestry: readonly AncestryEntry[];
  /** All known topology keys from the manifest. */
  keys: string[];
  /** Check if a key is a known topology (vs human/external). */
  isNode(key: string): boolean;
  /** Check ancestry constraints for a target. Returns rejection reason or null if allowed. */
  checkAncestry(target: string): string | null;
  /** Access a previously executed node's layers and output. Returns null if not yet executed. */
  node(key: string): { layers: Layer[]; output: unknown } | null;
  /** Unique ID for this exec$() invocation. */
  readonly invocationId: string;
  /** Unique ID for this graph execution. */
  readonly graphId: string;
  /** Unique ID for this node execution instance. */
  readonly nodeId: string;
  /** Session id from the top-level exec$ opts. Null when no session was seeded. */
  readonly sessionId: string | null;
  /** Session origin from the top-level exec$ opts. Null when no session was seeded. */
  readonly sessionOrigin: string | null;
}

/** Thrown when the traversal is aborted via AbortSignal. */
export class Aborted extends Error {
  constructor() {
    super("aborted");
  }
}

/** Denied action — thrown by exec$ when policy says no. */
export class Denied extends Error {
  constructor(
    public readonly node: string,
    public readonly oracle: string,
    public readonly reason: string,
  ) {
    super(`denied: ${node} on ${oracle} — ${reason}`);
  }
}

// ═══════════════════════════════════════════════════════════
// BLUEPRINT & TOPOLOGY
// ═══════════════════════════════════════════════════════════

/** Predefined graph — nodes and edges. */
export interface Graph {
  readonly nodes: string[];
  readonly edges: Array<{ from: string; to: string }>;
}

/** map metadata — dynamic fan-out over an array from upstream output. */
export interface MapConfig {
  /** Key in upstream output to iterate over. */
  readonly items: string;
  /** Sub-topology to execute per item. */
  readonly subTopology: Graph;
}

/** Consumer's manifest interface — loaded from specs at boot. */
export interface Manifest {
  getNode(key: string): Node;
  getNodeKeys(): string[];
  isNode(key: string): boolean;
  getGraph(key: string): Graph;
  /** Per-graph policy overrides. Returns null to fall back to global. */
  getGraphPolicies?(key: string): Policy[] | null;
  /** Per-node policy overrides. Returns null to fall back to global. */
  getNodePolicies?(nodeKey: string): Policy[] | null;
  /** Consumer-provided ID generator. Called before firing any events for the entity. */
  newId?(entity: "manifest" | "graph" | "node" | "oracle" | "policy"): string;
  /** map metadata for a node. Returns null if node is not a map. */
  getMap?(nodeKey: string): MapConfig | null;
}

// ═══════════════════════════════════════════════════════════
// SIGNALS — every side effect visible in the stream
// ═══════════════════════════════════════════════════════════

/** Identity of a node in the ancestry chain. */
export type AncestryEntry = {
  readonly nodeKey: string;
  readonly nodeId: string;
  readonly graphId: string;
  readonly oracleId: string;
  readonly policyId: string;
};

export type SignalBase = {
  readonly nodeKey: string;
  readonly graphId: string;
  readonly ancestry: readonly AncestryEntry[];
  readonly invocationId: string;
  readonly nodeId: string;
  readonly oracleId: string;
  readonly policyId: string;
  readonly timestamp: number;
};

/** Discriminated body of a Signal — everything except the SignalBase envelope. */
export type SignalBody =
  // graph lifecycle
  | { type: "graph:before"; key: string; nodeCount: number }
  | { type: "graph:after"; key: string }
  | { type: "graph:error"; key: string; error: string }
  // node lifecycle
  | { type: "node:before" }
  | { type: "node:after"; output: unknown }
  | { type: "node:error"; error: string }
  // oracle exec lifecycle — emitted on every execAction$ call
  | { type: "oracle:exec:in"; oracle: string }
  | { type: "oracle:exec:out"; oracle: string; durationMs: number; output?: unknown }
  // oracle denied — custom policy denied an action
  | { type: "oracle:denied"; oracle: string; reason: string }
  // oracle exceeded — limit exceeded pre-exec
  | { type: "oracle:exceeded"; oracle: string; limitType: string; accumulated: number; max: number; mutable: boolean }
  // oracle interrupts
  | { type: "oracle:interrupt:in"; payload: unknown }
  | { type: "oracle:interrupt:out"; payload: unknown; decision: string }
  // oracle policy — limit checks post-exec
  | {
      type: `oracle:policy:${string}`;
      limitType: string;
      current: Record<string, number>;
      total: number;
      accumulated: number;
      max: number;
      window?: TimeWindow;
      approval: string;
    }
  // oracle mutation — limit mutated at runtime
  | {
      type: `oracle:mutation:${string}`;
      limitType: string;
      previousMax: number;
      newMax: number;
      approvedBy: string;
    }
  // consumer extension namespace — engine never emits these
  | { type: `x:${string}`; [key: string]: unknown };

export type Signal = SignalBase & SignalBody;

// ═══════════════════════════════════════════════════════════
// LIMITS — PolicyExceeded error
// ═══════════════════════════════════════════════════════════

export class PolicyExceeded extends Error {
  constructor(
    public readonly label: string,
    public readonly value: number,
    public readonly limit: number,
  ) {
    super(`limit:${label} — ${value} >= ${limit}`);
  }
}

// ═══════════════════════════════════════════════════════════
// CONSTRAINTS — shipped as CustomPolicy factories
// ═══════════════════════════════════════════════════════════

export const constraints = {
  /** Blocks if any node appears in ancestry more than `max` times. */
  maxCycles: (max: number): CustomPolicy => ({
    id: `constraint:maxCycles:${max}`,
    policy: (action: PolicyAction): PolicyResult => {
      if (!action.target) return { approval: "allow" };
      const count = action.ancestry.filter((a) => a.nodeKey === action.target).length;
      return count > max
        ? { approval: "deny", reason: `${action.target} revisited ${count}/${max} times` }
        : { approval: "allow" };
    },
  }),

  /** Blocks if ancestry chain exceeds max depth. */
  maxDepth: (max: number): CustomPolicy => ({
    id: `constraint:maxDepth:${max}`,
    policy: (action: PolicyAction): PolicyResult =>
      action.ancestry.length >= max
        ? { approval: "deny", reason: `max depth (${max}) exceeded` }
        : { approval: "allow" },
  }),

  /** Allow a specific node to loop up to `max` times. Stops bubbling so global maxCycles doesn't override. */
  maxLoop: (node: string, max: number): CustomPolicy => ({
    id: `constraint:maxLoop:${node}:${max}`,
    policy: (action: PolicyAction): PolicyResult => {
      if (action.target !== node) return { approval: "allow" };
      const count = action.ancestry.filter((a) => a.nodeKey === node).length;
      return count <= max
        ? { approval: "allow", stop: true }
        : { approval: "deny", reason: `${node} max ${max} loops` };
    },
  }),
};

// ═══════════════════════════════════════════════════════════
// CORE TYPES
// ═══════════════════════════════════════════════════════════

/** Resolved interrupt result (engine internal). */
export type ResolvedInterrupt =
  | { decision: "retry"; signals: Signal[] }
  | { decision: "deny"; reason: string; signals: Signal[] }
  | { decision: "result"; value: unknown; signals: Signal[] }
  | { decision: "mutate"; oracle: string; limitType: string; max: number; signals: Signal[] };

// ═══════════════════════════════════════════════════════════
// INTERNAL TYPES
// ═══════════════════════════════════════════════════════════

interface GraphExecCtx {
  readonly invocationId: string;
  readonly graphId: string;
  readonly nodeId: string;
  readonly oracleId: string;
  readonly policyId: string;
  readonly manifest: Manifest;
  readonly topology: Graph;
  readonly ancestry: readonly AncestryEntry[];
  readonly oracles: Readonly<Record<string, Oracle>>;
  readonly globalPolicies: readonly CustomPolicy[];
  readonly handleInterrupt?: (interrupt: Interrupt, ctx: InterruptCtx) => Promise<InterruptResult>;
  readonly interruptTimeout: number;
  readonly signal?: AbortSignal;
  readonly policies: PolicyState;
  readonly nodeState: Map<string, { layers: Layer[]; output: unknown }>;
  readonly sessionId: string | null;
  readonly sessionOrigin: string | null;
}

/** Extract PolicyScope from GraphExecCtx — builds the full graph chain from ancestry + current. */
function scopeOf(ctx: GraphExecCtx): PolicyScope {
  const graphIds: string[] = [];
  for (const a of ctx.ancestry) {
    if (!graphIds.includes(a.graphId)) graphIds.push(a.graphId);
  }
  if (!graphIds.includes(ctx.graphId)) graphIds.push(ctx.graphId);
  return { invocationId: ctx.invocationId, graphIds };
}

/** Tracks action history within a scope. */
class ActionScope {
  private static readonly MAX = 200;
  private readonly _buf: string[] = new Array(ActionScope.MAX);
  private _head = 0;
  private _size = 0;

  record(oracle: string): void {
    const idx = (this._head + this._size) % ActionScope.MAX;
    if (this._size < ActionScope.MAX) {
      this._size++;
    } else {
      this._head = (this._head + 1) % ActionScope.MAX;
    }
    this._buf[idx] = oracle;
  }

  get history(): readonly string[] {
    const result: string[] = new Array(this._size);
    for (let i = 0; i < this._size; i++) {
      result[i] = this._buf[(this._head + i) % ActionScope.MAX]!;
    }
    return result;
  }
}

// ═══════════════════════════════════════════════════════════
// POLICY STATE — tracks accumulated counters per
// (oracleType, limitType, window) with time-window support.
// Core foundation — persistence via snapshot/restore.
// ═══════════════════════════════════════════════════════════

/** Serializable snapshot of all policy counters. For persistence/recovery. */
export type PolicySnapshot = ReadonlyArray<{
  key: string;
  accumulated: number;
  max: number;
  originalMax: number;
  windowStart: number;
}>;

function windowToMs(w: TimeWindow): number | null {
  if (typeof w === "number") return w;
  switch (w) {
    case "minute": return 60_000;
    case "hour": return 3_600_000;
    case "day": return 86_400_000;
    case "week": return 604_800_000;
    case "month": return 2_592_000_000; // 30 days
    case "graph": return null;
    case "invocation": return null;
  }
}

interface CounterEntry {
  accumulated: number;
  windowStart: number;
  currentMax: number;
  originalMax: number;
}

/**
 * Scope IDs for transient counter isolation. Threaded through the call chain.
 *
 * graphIds is the full nesting chain from outermost to innermost.
 * Graph-scoped limits accumulate up the chain — same model as oracle key hierarchy.
 * A call in graph C (spawned by B, spawned by A) ticks A's, B's, AND C's counters.
 */
interface PolicyScope {
  readonly invocationId: string;
  readonly graphIds: readonly string[];
}

class PolicyState {
  private counters = new Map<string, CounterEntry>();
  /** Tracks which graphIds belong to each invocation — for cleanup. */
  private invocationGraphs = new Map<string, Set<string>>();

  /** Register a graphId as belonging to an invocation. */
  registerGraph(invocationId: string, graphId: string): void {
    let set = this.invocationGraphs.get(invocationId);
    if (!set) { set = new Set(); this.invocationGraphs.set(invocationId, set); }
    set.add(graphId);
  }

  /** Clean up all transient entries for a completed invocation. */
  cleanupInvocation(invocationId: string): void {
    const invSuffix = `:invocation:${invocationId}`;
    const graphIds = this.invocationGraphs.get(invocationId);
    for (const k of [...this.counters.keys()]) {
      if (k.endsWith(invSuffix)) { this.counters.delete(k); continue; }
      if (graphIds) {
        for (const gid of graphIds) {
          if (k.endsWith(`:graph:${gid}`)) { this.counters.delete(k); break; }
        }
      }
    }
    this.invocationGraphs.delete(invocationId);
  }

  private key(oracleType: string, limitType: string, window: TimeWindow | undefined, graphId: string, invocationId: string): string {
    if (window === undefined || window === "invocation") {
      return `${oracleType}:${limitType}:invocation:${invocationId}`;
    }
    if (window === "graph") {
      return `${oracleType}:${limitType}:graph:${graphId}`;
    }
    return `${oracleType}:${limitType}:${window}`;
  }

  /** Get or create a counter entry for a specific graphId. */
  private getEntry(oracleType: string, limit: OraclePolicy, graphId: string, invocationId: string): CounterEntry {
    const k = this.key(oracleType, limit.type, limit.window, graphId, invocationId);
    let entry = this.counters.get(k);
    if (!entry) {
      entry = { accumulated: 0, windowStart: Date.now(), currentMax: limit.max, originalMax: limit.max };
      this.counters.set(k, entry);
    }
    // Check time window expiry
    const ms = limit.window != null ? windowToMs(limit.window) : null;
    if (ms != null && Date.now() - entry.windowStart > ms) {
      entry.accumulated = 0;
      entry.windowStart = Date.now();
      entry.currentMax = entry.originalMax;
    }
    return entry;
  }

  /**
   * Check a limit across the full graph chain.
   * For graph-scoped limits, checks ALL ancestor graphIds — if any is exceeded, it's exceeded.
   * Returns the most restrictive (highest accumulated / lowest max).
   */
  checkLimit(oracleType: string, limit: OraclePolicy, scope: PolicyScope): { exceeded: boolean; accumulated: number; max: number } {
    if (limit.window === "graph") {
      let worst = { exceeded: false, accumulated: 0, max: limit.max };
      for (const gid of scope.graphIds) {
        const entry = this.getEntry(oracleType, limit, gid, scope.invocationId);
        const check = { exceeded: entry.accumulated >= entry.currentMax, accumulated: entry.accumulated, max: entry.currentMax };
        if (check.exceeded) return check;
        if (check.accumulated > worst.accumulated) worst = check;
      }
      return worst;
    }
    const entry = this.getEntry(oracleType, limit, scope.graphIds[scope.graphIds.length - 1]!, scope.invocationId);
    return { exceeded: entry.accumulated >= entry.currentMax, accumulated: entry.accumulated, max: entry.currentMax };
  }

  /**
   * Accumulate a value for a limit.
   * For graph-scoped limits, accumulates against ALL ancestor graphIds.
   * Returns the innermost graph's counter entry (for signal reporting).
   */
  accumulate(oracleType: string, limit: OraclePolicy, amount: number, scope: PolicyScope): CounterEntry {
    if (limit.window === "graph") {
      let innermost!: CounterEntry;
      for (const gid of scope.graphIds) {
        const entry = this.getEntry(oracleType, limit, gid, scope.invocationId);
        entry.accumulated += amount;
        innermost = entry;
      }
      return innermost;
    }
    const entry = this.getEntry(oracleType, limit, scope.graphIds[scope.graphIds.length - 1]!, scope.invocationId);
    entry.accumulated += amount;
    return entry;
  }

  /** Mutate a limit's max. For graph-scoped, mutates the innermost graph's counter. Returns previous max. */
  mutateMax(oracleType: string, limitType: string, window: TimeWindow | undefined, newMax: number, scope: PolicyScope): number {
    const gid = scope.graphIds[scope.graphIds.length - 1]!;
    const k = this.key(oracleType, limitType, window, gid, scope.invocationId);
    const entry = this.counters.get(k);
    if (!entry) return 0;
    const prev = entry.currentMax;
    entry.currentMax = newMax;
    return prev;
  }

  /** Find a limit entry by exact key. */
  findEntryByWindow(oracleType: string, limitType: string, window: TimeWindow | undefined, scope: PolicyScope): CounterEntry | undefined {
    const gid = scope.graphIds[scope.graphIds.length - 1]!;
    return this.counters.get(this.key(oracleType, limitType, window, gid, scope.invocationId));
  }

  /** Reset counters scoped to a specific graph execution (only the new graph, not ancestors). */
  resetGraphWindow(graphId: string): void {
    const suffix = `:graph:${graphId}`;
    for (const [k, entry] of this.counters) {
      if (k.endsWith(suffix)) {
        entry.accumulated = 0;
        entry.windowStart = Date.now();
        entry.currentMax = entry.originalMax;
      }
    }
  }

  /** Reset counters scoped to a specific invocation. */
  resetInvocationWindow(invocationId: string): void {
    const suffix = `:invocation:${invocationId}`;
    for (const [k, entry] of this.counters) {
      if (k.endsWith(suffix)) {
        entry.accumulated = 0;
        entry.windowStart = Date.now();
        entry.currentMax = entry.originalMax;
      }
    }
  }

  /** Query counters for a specific oracle, walking the key hierarchy. Uses innermost graphId. */
  countersFor(oracleKey: string, oracles: Readonly<Record<string, Oracle>>, scope: PolicyScope): Array<{ type: string; accumulated: number; max: number; window?: TimeWindow }> {
    const results: Array<{ type: string; accumulated: number; max: number; window?: TimeWindow }> = [];
    const gid = scope.graphIds[scope.graphIds.length - 1]!;
    const chain = oracleKey.split(":");
    for (let i = chain.length; i >= 1; i--) {
      const key = chain.slice(0, i).join(":");
      const ot = oracles[key];
      if (!ot) continue;
      for (const p of ot.policies) {
        if (!isOraclePolicy(p)) continue;
        if (p.type === "timeout" || p.type === "retries") continue;
        const k = this.key(key, p.type, p.window, gid, scope.invocationId);
        const entry = this.counters.get(k);
        results.push({
          type: p.type,
          accumulated: entry?.accumulated ?? 0,
          max: entry?.currentMax ?? p.max,
          window: p.window,
        });
      }
    }
    return results;
  }

  /** Snapshot only persistent (time-windowed) counters. Transient entries (invocation/graph) are excluded. */
  snapshot(): PolicySnapshot {
    const result: Array<PolicySnapshot[number]> = [];
    for (const [k, entry] of this.counters) {
      if (k.includes(":invocation:") || k.includes(":graph:")) continue;
      result.push({
        key: k,
        accumulated: entry.accumulated,
        max: entry.currentMax,
        originalMax: entry.originalMax,
        windowStart: entry.windowStart,
      });
    }
    return result;
  }

  /** Restore counters from a previous snapshot. */
  restore(snapshot: PolicySnapshot): void {
    for (const entry of snapshot) {
      const counterEntry: CounterEntry = {
        accumulated: entry.accumulated,
        currentMax: entry.max,
        originalMax: entry.originalMax,
        windowStart: entry.windowStart,
      };
      this.counters.set(entry.key, counterEntry);
    }
  }
}

// ═══════════════════════════════════════════════════════════
// POLICY ENGINE — two-phase resolution with hierarchy
// ═══════════════════════════════════════════════════════════

/** Walk oracle key hierarchy by splitting on `:`. Returns keys from most specific to least. */
function oracleKeyChain(oracleKey: string): string[] {
  const parts = oracleKey.split(":");
  const chain: string[] = [];
  for (let i = parts.length; i >= 1; i--) {
    chain.push(parts.slice(0, i).join(":"));
  }
  return chain;
}

/** A policy tagged with the oracle key that owns it — avoids reference-equality scans. */
interface CollectedPolicy {
  policy: Policy;
  origin: string;
}

/** Collect all policies for an execAction$ call in bubble order (oracle chain → node → graph → global). */
function collectPolicies(
  oracleKey: string,
  nodeKey: string,
  oracles: Readonly<Record<string, Oracle>>,
  manifest: Manifest,
  globalPolicies: readonly CustomPolicy[],
  ancestry: readonly AncestryEntry[],
): CollectedPolicy[] {
  const all: CollectedPolicy[] = [];

  // OracleExec hierarchy (most specific first)
  const chain = oracleKeyChain(oracleKey);
  for (const key of chain) {
    const ot = oracles[key];
    if (ot) {
      for (const p of ot.policies) all.push({ policy: p, origin: key });
    }
  }

  // Node policies
  const nodePolicies = manifest.getNodePolicies?.(nodeKey);
  if (nodePolicies) {
    for (const p of nodePolicies) all.push({ policy: p, origin: `node:${nodeKey}` });
  }

  // Graph policies (use root agent from ancestry)
  const rootAgent = ancestry.length > 0 ? ancestry[0]!.nodeKey : nodeKey;
  const graphPolicies = manifest.getGraphPolicies?.(rootAgent);
  if (graphPolicies) {
    for (const p of graphPolicies) all.push({ policy: p, origin: `graph:${rootAgent}` });
  }

  // Global policies
  for (const p of globalPolicies) all.push({ policy: p, origin: "global" });

  return all;
}

/** Run two-phase policy resolution. Returns the final result.
 *
 *  Policies array is in bubble order: oracle (specific→root) → node → graph → global.
 *  Capture phase reverses this (global first = highest authority).
 */
function resolvePolicies(collected: CollectedPolicy[], action: PolicyAction): PolicyResult {
  // Phase 1: CAPTURE (top-down — global first, highest authority wins)
  for (let i = collected.length - 1; i >= 0; i--) {
    const { policy: p } = collected[i]!;
    if (!isCustomPolicy(p)) continue;
    const r = p.policy(action);
    if ("force" in r && r.force) return r;
  }

  // Phase 2: BUBBLE (bottom-up — oracle first, first deny/stop wins)
  for (const { policy: p } of collected) {
    if (!isCustomPolicy(p)) continue;
    const r = p.policy(action);
    if (r.approval === "deny") return r;
    if (r.approval === "interrupt") return r;
    if (r.approval === "allow" && "stop" in r && r.stop) return r;
  }

  return { approval: "allow" };
}

/** Collect all OraclePolicys from the policy chain with their origin oracle key. */
function collectLimits(collected: CollectedPolicy[]): Array<{ limit: OraclePolicy; origin: string }> {
  const results: Array<{ limit: OraclePolicy; origin: string }> = [];
  for (const { policy, origin } of collected) {
    if (isOraclePolicy(policy)) results.push({ limit: policy, origin });
  }
  return results;
}


// ═══════════════════════════════════════════════════════════
// SIGNAL HELPERS — typed signal factory
// ═══════════════════════════════════════════════════════════

/** Stamp a signal with the common envelope. */
function emit(ctx: GraphExecCtx, nodeKey: string, body: SignalBody): Signal {
  return {
    nodeKey, graphId: ctx.graphId, ancestry: ctx.ancestry,
    invocationId: ctx.invocationId, nodeId: ctx.nodeId, oracleId: ctx.oracleId, policyId: ctx.policyId,
    timestamp: Date.now(),
    ...body,
  } as Signal;
}

// ═══════════════════════════════════════════════════════════
// execAction$ — the ONE function that wraps every action
//
// Policies are collected ONCE per call and threaded through.
// exec:in is emitted once (not on retries). exec:out is
// emitted on both success and error via finalize.
// ═══════════════════════════════════════════════════════════

function execAction$<T>(
  nodeKey: string,
  oracleType: string,
  input: unknown,
  execute: () => Observable<T>,
  scope: ActionScope,
  ctx: GraphExecCtx,
  signals$: Subject<Signal>,
): Observable<T> {
  const newId = resolveNewId(ctx.manifest);
  const oracleCtx = { ...ctx, oracleId: newId("oracle") };
  const execStart = Date.now();
  let execInEmitted = false;
  let execOutEmitted = false;

  // Collect policies ONCE — reused for resolution, limits, retries, post-exec
  const collected = collectPolicies(
    oracleType, nodeKey, ctx.oracles, ctx.manifest, ctx.globalPolicies, ctx.ancestry,
  );
  const limits = collectLimits(collected);
  const retriesLimit = limits.find(({ limit: l }) => l.type === "retries");
  const timeoutLimit = limits.find(({ limit: l }) => l.type === "timeout");
  const timeoutMs = timeoutLimit?.limit.max ?? 0;
  const retryCount = retriesLimit?.limit.max ?? 0;
  const backoffConfig = retriesLimit?.limit.backoff;

  let policyCtx = oracleCtx; // updated per defer() invocation (retries get fresh policyId)

  return defer(() => {
    // Abort check
    if (ctx.signal?.aborted) throw new Aborted();

    // Emit oracle:exec:in only once (not on retries)
    if (!execInEmitted) {
      signals$.next(emit(oracleCtx, nodeKey, { type: "oracle:exec:in", oracle: oracleType }));
      execInEmitted = true;
    }

    // Generate policyId for this resolution pass
    policyCtx = { ...oracleCtx, policyId: newId("policy") };

    // Build PolicyAction
    const action: PolicyAction = {
      node: nodeKey,
      oracle: oracleType,
      input,
      history: scope.history,
      ancestry: oracleCtx.ancestry,
      timestamp: Date.now(),
      invocationId: oracleCtx.invocationId,
      graphId: oracleCtx.graphId,
      nodeId: oracleCtx.nodeId,
      oracleId: oracleCtx.oracleId,
      policyId: policyCtx.policyId,
    };

    // 1. Two-phase custom policy resolution
    const policyResult = resolvePolicies(collected, action);

    if (policyResult.approval === "deny") {
      const reason = "reason" in policyResult ? policyResult.reason : "denied";
      signals$.next(emit(policyCtx, nodeKey, { type: "oracle:denied", oracle: oracleType, reason }));
      throw new Denied(nodeKey, oracleType, reason);
    }
    if (policyResult.approval === "interrupt") {
      throw (policyResult as { approval: "interrupt"; interrupt: Interrupt }).interrupt;
    }

    // 2. Pre-exec limit checks (using origin from collectLimits — no reference equality)
    for (const { limit, origin } of limits) {
      if (limit.type === "timeout" || limit.type === "retries") continue;

      const check = ctx.policies.checkLimit(origin, limit, scopeOf(ctx));
      if (check.exceeded) {
        const isMutable = !!(limit.mutable && Object.keys(limit.mutable).length > 0);
        signals$.next(emit(policyCtx, nodeKey, {
          type: "oracle:exceeded", oracle: origin, limitType: limit.type,
          accumulated: check.accumulated, max: check.max, mutable: isMutable,
        }));
        if (isMutable) {
          throw new Interrupt({
            type: "limit_exceeded",
            oracleType: origin,
            limitType: limit.type,
            window: limit.window,
            accumulated: check.accumulated,
            max: check.max,
            mutable: limit.mutable,
          });
        }
        throw new PolicyExceeded(`${origin}:${limit.type}`, check.accumulated, check.max);
      }
    }

    scope.record(oracleType);

    // 3. Execute — executor returns Observable<T>
    const obs$ = execute();
    return timeoutMs > 0 ? obs$.pipe(timeout(timeoutMs)) : obs$;
  }).pipe(
    retry({
      count: retryCount,
      delay: (err, attempt) => {
        if (err instanceof Denied || err instanceof Interrupt || err instanceof PolicyExceeded || err instanceof Aborted) throw err;
        if (!backoffConfig) return of(null);
        const ms = typeof backoffConfig === "number" ? backoffConfig
          : backoffConfig === "exponential" ? 1000 * Math.pow(2, attempt - 1)
          : backoffConfig(attempt - 1);
        return timer(ms);
      },
    }),
    tap((value) => {
      // Post-exec: extract and accumulate limits (using pre-collected limits with origin)
      for (const { limit, origin } of limits) {
        if (limit.type === "timeout" || limit.type === "retries") continue;

        let current: Record<string, number>;
        let total: number;

        if (limit.type === "calls") {
          current = { calls: 1 };
          total = 1;
        } else if (limit.extract) {
          current = limit.extract(value);
          total = Object.values(current).reduce((a, b) => a + b, 0);
        } else {
          continue;
        }

        const entry = ctx.policies.accumulate(origin, limit, total, scopeOf(ctx));

        // Emit oracle:policy signal
        signals$.next(emit(policyCtx, nodeKey, {
          type: `oracle:policy:${origin}`,
          limitType: limit.type,
          current,
          total,
          accumulated: entry.accumulated,
          max: entry.currentMax,
          window: limit.window,
          approval: entry.accumulated >= entry.currentMax ? "exceeded" : "allow",
        }));
      }
    }),
    tap((value) => {
      // oracle:exec:out on success — carries the produced output so
      // subscribers can observe what each oracle call returned without
      // querying the events table separately.
      execOutEmitted = true;
      signals$.next(emit(oracleCtx, nodeKey, {
        type: "oracle:exec:out", oracle: oracleType, durationMs: Date.now() - execStart,
        output: value,
      }));
    }),
    // Emit oracle:exec:out on error before re-throwing
    catchError((err) => {
      if (execInEmitted && !execOutEmitted) {
        execOutEmitted = true;
        signals$.next(emit(oracleCtx, nodeKey, {
          type: "oracle:exec:out", oracle: oracleType, durationMs: Date.now() - execStart,
        }));
      }
      throw err;
    }),
  );
}

// ═══════════════════════════════════════════════════════════
// resolveInterrupt$ — calls consumer's handleInterrupt with
// configurable timeout, mutation support
// ═══════════════════════════════════════════════════════════

function resolveInterrupt$(
  interrupt: Interrupt,
  nodeKey: string,
  ctx: GraphExecCtx,
): Observable<ResolvedInterrupt> {
  const { handleInterrupt, interruptTimeout } = ctx;
  const newId = resolveNewId(ctx.manifest);
  if (!handleInterrupt) {
    return of({
      decision: "deny" as const,
      reason: "no handleInterrupt configured",
      signals: [emit(ctx, nodeKey, { type: "oracle:interrupt:in", payload: interrupt.payload }),
                emit(ctx, nodeKey, { type: "oracle:interrupt:out", payload: interrupt.payload, decision: "deny:no-handler" })],
    });
  }

  const intInSignal = emit(ctx, nodeKey, { type: "oracle:interrupt:in", payload: interrupt.payload });

  const graphFn = (opts: { key: string; data: unknown }) =>
    executeGraph$(
      { ...ctx, graphId: newId("graph"), topology: ctx.manifest.getGraph(opts.key), ancestry: [...ctx.ancestry, { nodeKey: opts.key, nodeId: ctx.nodeId, graphId: ctx.graphId, oracleId: ctx.oracleId, policyId: ctx.policyId }] },
      typeof opts.data === "string" ? opts.data : JSON.stringify(opts.data),
    );

  const handlePromise = Promise.race([
    handleInterrupt(interrupt, { spawnSync: graphFn, node: nodeKey, ancestry: ctx.ancestry }),
    new Promise<never>((_, reject) => setTimeout(() => reject(new Error("handleInterrupt timeout")), interruptTimeout)),
  ]);

  return from(handlePromise).pipe(
    concatMap((result): Observable<ResolvedInterrupt> => {
      const outSignal = emit(ctx, nodeKey, { type: "oracle:interrupt:out", payload: interrupt.payload, decision: result.decision });
      if (result.decision === "retry") {
        return of({ decision: "retry" as const, signals: [intInSignal, outSignal] });
      }
      if (result.decision === "deny") {
        return of({ decision: "deny" as const, reason: result.reason, signals: [intInSignal, outSignal] });
      }
      if (result.decision === "mutate") {
        // Validate mutation against mutable permissions on the limit
        // payload.oracleType = the oracle whose limit was exceeded (the limit owner)
        // result.oracle = the approver oracle key (who authorized the mutation)
        const payload = interrupt.payload as { oracleType?: string; limitType?: string; window?: TimeWindow; mutable?: Record<string, true | ((next: number, current: number, max: number) => boolean)> };
        const approverKey = result.oracle;
        const targetLimitType = result.limitType;
        const requestedMax = result.max;
        const limitOwner = payload.oracleType!;

        const mutableConfig = payload.mutable;
        if (!mutableConfig) {
          return of({ decision: "deny" as const, reason: "limit is not mutable", signals: [intInSignal, outSignal] });
        }

        const permission = mutableConfig[approverKey];
        if (permission === undefined) {
          return of({ decision: "deny" as const, reason: `oracle ${approverKey} not authorized to mutate ${targetLimitType}`, signals: [intInSignal, outSignal] });
        }

        if (permission !== true) {
          // Look up the limit owner's counter (not the approver's)
          // Always use scoped lookup — findEntry without scope could match stale counters
          const entry = ctx.policies.findEntryByWindow(limitOwner, targetLimitType, payload.window, scopeOf(ctx));
          const currentAccumulated = entry?.accumulated ?? 0;
          const currentMax = entry?.currentMax ?? 0;
          if (!permission(requestedMax, currentAccumulated, currentMax)) {
            return of({ decision: "deny" as const, reason: `mutation to ${requestedMax} rejected by ceiling function`, signals: [intInSignal, outSignal] });
          }
        }

        // Use window from interrupt payload (set when the limit was exceeded),
        // falling back to oracle search for backwards compatibility
        let limitWindow: TimeWindow | undefined = payload.window;
        if (limitWindow === undefined) {
          const oracleChain = oracleKeyChain(limitOwner);
          for (const key of oracleChain) {
            const ot = ctx.oracles[key];
            if (!ot) continue;
            const limit = ot.policies.filter(isOraclePolicy).find((l) => l.type === targetLimitType);
            if (limit) { limitWindow = limit.window; break; }
          }
        }

        const previousMax = ctx.policies.mutateMax(
          limitOwner, targetLimitType, limitWindow, requestedMax, scopeOf(ctx),
        );

        const mutationSignal = emit(ctx, nodeKey, {
          type: `oracle:mutation:${limitOwner}`,
          limitType: targetLimitType,
          previousMax,
          newMax: requestedMax,
          approvedBy: approverKey,
        });

        return of({
          decision: "mutate" as const,
          oracle: limitOwner,
          limitType: targetLimitType,
          max: requestedMax,
          signals: [intInSignal, mutationSignal, outSignal],
        });
      }
      // result
      return of({ decision: "result" as const, value: result.value, signals: [intInSignal, outSignal] });
    }),
    catchError((handlerErr) => of({
      decision: "deny" as const,
      reason: `handleInterrupt failed: ${handlerErr instanceof Error ? handlerErr.message : String(handlerErr)}`,
      signals: [intInSignal, emit(ctx, nodeKey, { type: "oracle:interrupt:out", payload: interrupt.payload, decision: "error" })],
    })),
  );
}

// ═══════════════════════════════════════════════════════════
// executeNode$ — one node's full execution
//
// Consumer owns the execution loop. Engine provides NodeExec
// with exec$, oracle, spawnSync, spawnAsync, handleInterrupt.
//
// Signals from execAction$ are emitted eagerly via nodeSignals$
// (a Subject) so they reach runtime.signals$ — and therefore WS
// clients — immediately as they're produced. The Subject is
// merged into the node's output Observable alongside node:before,
// node:after, and background tasks.
// ═══════════════════════════════════════════════════════════

function executeNode$(
  ctx: GraphExecCtx,
  nodeKey: string,
  input: unknown,
  scope?: ActionScope,
): Observable<Signal> {
  const newId = resolveNewId(ctx.manifest);
  const nodeCtxId = { ...ctx, nodeId: newId("node") };
  const spec = ctx.manifest.getNode(nodeKey);
  const nodeScope = scope ?? new ActionScope();
  const backgroundTasks: Observable<Signal>[] = [];
  const nodeSignals$ = new Subject<Signal>();

  const nestedGraph$ = (opts: { key: string; data: unknown }): Observable<Signal> =>
    executeGraph$(
      { ...nodeCtxId, graphId: newId("graph"), topology: ctx.manifest.getGraph(opts.key), ancestry: [...ctx.ancestry, { nodeKey: opts.key, nodeId: nodeCtxId.nodeId, graphId: nodeCtxId.graphId, oracleId: nodeCtxId.oracleId, policyId: nodeCtxId.policyId }] },
      opts.data,
    );

  // Check ancestry for spawning — uses constraint policies from the full chain
  const checkAncestryFn = (target: string): string | null => {
    const action: PolicyAction = {
      node: nodeKey,
      oracle: "",
      input: undefined,
      history: nodeScope.history,
      ancestry: nodeCtxId.ancestry,
      target,
      timestamp: Date.now(),
      invocationId: nodeCtxId.invocationId,
      graphId: nodeCtxId.graphId,
      nodeId: nodeCtxId.nodeId,
      oracleId: "",
      policyId: "",
    };

    const nodePolicies = ctx.manifest.getNodePolicies?.(target) ?? [];
    const rootAgent = nodeCtxId.ancestry.length > 0 ? nodeCtxId.ancestry[0]!.nodeKey : nodeKey;
    const graphPolicies = ctx.manifest.getGraphPolicies?.(rootAgent) ?? [];
    const globalPolicies = [...ctx.globalPolicies];
    const allPolicies = [...nodePolicies, ...graphPolicies, ...globalPolicies];

    for (const p of allPolicies) {
      if (!isCustomPolicy(p)) continue;
      const r = p.policy(action);
      if (r.approval === "deny") return "reason" in r ? r.reason : "denied by policy";
      if (r.approval === "allow" && "stop" in r && r.stop) return null;
    }
    return null;
  };

  // Shared exec$ — used by both nodeCtx and oracle. Captures signals on success AND error.
  // `fn` must return an Observable. Promises are NOT accepted — wrap them at the
  // boundary with from() or defer(async ...).
  const ctxExec$ = <T>(
    oracleType: string,
    fn: () => Observable<T>,
    input?: unknown,
  ): Observable<T> => {
    const oracleDef = (() => {
      const chain = oracleKeyChain(oracleType);
      for (const key of chain) {
        if (ctx.oracles[key]) return ctx.oracles[key];
      }
      return undefined;
    })();
    if (!oracleDef) throw new Error(`unknown oracle type: ${oracleType}`);

    return execAction$(
      nodeKey, oracleType, input,
      () => defer(() => {
        if (ctx.signal?.aborted) throw new Aborted();
        return fn();
      }),
      nodeScope, nodeCtxId,
      nodeSignals$,
    );
  };

  // Build OracleExec — ergonomic wrappers around execAction$
  const oracle: OracleExec = {
    call<T>(oracle: string, fn: () => Observable<T>, input?: unknown): Observable<T> {
      return ctxExec$(oracle, fn, input);
    },

    llm(opts): Observable<NodeResult> {
      const llmOracleKey = opts.oracle;

      // Pre-collect round limits once for the loop — uses consumer's oracle key to walk hierarchy
      const oracleCollected = collectPolicies(
        llmOracleKey, nodeKey, ctx.oracles, ctx.manifest, ctx.globalPolicies, ctx.ancestry,
      );
      const roundLimits = collectLimits(oracleCollected).filter(({ limit: l }) => l.type === "rounds");


      return new Observable<NodeResult>((subscriber) => {
        (async () => {
          const currentLayers: Layer[] = [];
          const history: unknown[] = [];
          // Only collect consumer-extension signals (from executeTool results).
          // exec$ signals are already captured in nodeSignals via the tap/catchError above.
          const consumerSignals: Signal[] = [];

          // Loop until no tool calls remain. Consumer controls max rounds via
          // { type: "rounds", max: N } policy on the oracle. No hardcoded cap.
          for (;;) {
            if (subscriber.closed) return;

            // 1. LLM call via exec$ — uses consumer's oracle key for hierarchy accumulation
            const response = await lastValueFrom(
              ctxExec$(llmOracleKey, () => from(opts.call(currentLayers, history))),
            );

            // 2. No tools → done
            if (!response.toolCalls || response.toolCalls.length === 0) {
              subscriber.next({ output: response.response, signals: consumerSignals });
              subscriber.complete();
              return;
            }

            // 3. Tool calls via exec$
            for (const tc of response.toolCalls) {
              if (subscriber.closed) return;
              if (!opts.executeTool) {
                currentLayers.push({ name: "tool", content: "no executeTool provided" });
                continue;
              }
              try {
                const toolResult = await lastValueFrom(
                  ctxExec$("tool", () => from(opts.executeTool!(tc, nodeCtx)), tc),
                );
                const result = toolResult as { content: string; signals?: Signal[] };
                currentLayers.push({ name: "tool", content: result.content });
                // Only collect consumer-generated signals from tool results
                if (result.signals) consumerSignals.push(...result.signals);
              } catch (err) {
                if (err instanceof Denied) {
                  currentLayers.push({ name: "tool", content: JSON.stringify({ denied: true, reason: err.reason }) });
                  continue;
                }
                throw err;
              }
            }

            // Accumulate "rounds" type limits (using pre-collected round limits with origin)
            // If max is exceeded, the next ctxExec$ call will throw PolicyExceeded.
            for (const { limit: rl, origin } of roundLimits) {
              ctx.policies.accumulate(origin, rl, 1, scopeOf(ctx));
            }

            history.push({ response });
          }
        })().catch((err) => subscriber.error(err));
      });
    },
  };

  // Build NodeExec — the consumer's full runtime API surface
  const nodeCtx: NodeExec = {
    counters: (oracleKey: string) => ctx.policies.countersFor(oracleKey, ctx.oracles, scopeOf(ctx)),
    exec$<T>(oracle: string, fnOrInput?: (() => Observable<T>) | unknown, input?: unknown): Observable<T> {
      if (typeof fnOrInput === "function") {
        return ctxExec$(oracle, fnOrInput as () => Observable<T>, input);
      }
      // No fn — look for registered executor on the oracle
      const chain = oracleKeyChain(oracle);
      for (const key of chain) {
        const def = ctx.oracles[key];
        if (def?.executor) {
          // Coerce whatever the executor returns into an Observable:
          //   - Observable passes through
          //   - Promise is wrapped with from()
          //   - Plain sync value is wrapped with of()
          //
          // Without this, executors that return plain objects (common
          // pattern for data-shaped oracles like a2ui components or
          // anything that just normalizes an input) crash downstream
          // with "You provided an invalid object where a stream was
          // expected" because ctxExec$ expects its fn() to return an
          // Observable and rxjs `defer` has no way to coerce on its own.
          return ctxExec$(oracle, () => {
            const result = def.executor!(fnOrInput, nodeCtx);
            if (result instanceof Observable) return result as Observable<T>;
            if (result && typeof (result as { then?: unknown }).then === "function") {
              return from(result as Promise<T>);
            }
            return of(result as T);
          }, fnOrInput);
        }
      }
      throw new Error(`oracle "${oracle}" has no registered executor — provide fn`);
    },
    oracle,
    spawnSync: nestedGraph$,
    spawnAsync: (opts: { key: string; data: unknown }) => {
      backgroundTasks.push(nestedGraph$(opts));
    },
    handleInterrupt: (interrupt: Interrupt) =>
      resolveInterrupt$(interrupt, nodeKey, nodeCtxId),
    ancestry: nodeCtxId.ancestry,
    keys: ctx.manifest.getNodeKeys(),
    isNode: (key: string) => ctx.manifest.isNode(key),
    checkAncestry: checkAncestryFn,
    node: (key: string) => ctx.nodeState.get(key) ?? null,
    invocationId: nodeCtxId.invocationId,
    graphId: nodeCtxId.graphId,
    nodeId: nodeCtxId.nodeId,
    sessionId: nodeCtxId.sessionId,
    sessionOrigin: nodeCtxId.sessionOrigin,
  };

  return defer(() => spec.loadPrompt(input, nodeCtx)).pipe(
    concatMap((layers) => {
      const execute$ = ctx.signal
        ? new Observable<NodeResult>((sub) => {
            if (ctx.signal!.aborted) { sub.error(new Aborted()); return; }
            const inner = spec.execute(layers, nodeCtx).subscribe({
              next: (v) => sub.next(v),
              error: (e) => { cleanup(); sub.error(e); },
              complete: () => { cleanup(); sub.complete(); },
            });
            const onAbort = () => { inner.unsubscribe(); sub.error(new Aborted()); };
            ctx.signal!.addEventListener("abort", onAbort, { once: true });
            function cleanup() { ctx.signal!.removeEventListener("abort", onAbort); }
          })
        : spec.execute(layers, nodeCtx);

      return execute$.pipe(
        concatMap((result) => {
          // Store node state for ctx.node() access by downstream nodes
          ctx.nodeState.set(nodeKey, { layers, output: result.output });
          // Complete the eager signal subject — all oracle signals for
          // this node have been emitted by now.
          nodeSignals$.complete();
          // Consumer-generated signals (from executeTool results) still
          // need to be emitted here since they bypass execAction$.
          const consumer_signals = result.signals ?? [];
          return merge(
            from(consumer_signals),
            of(emit(nodeCtxId, nodeKey, { type: "node:after", output: result.output })),
            ...backgroundTasks,
          );
        }),
      );
    }),

    // Prepend node:before, merge eager oracle signals alongside execution
    (source$) => merge(
      concat(of(emit(nodeCtxId, nodeKey, { type: "node:before" })), source$),
      nodeSignals$,
    ),

    catchError((err) => {
      const pending = [...backgroundTasks];
      backgroundTasks.length = 0;
      // Signals already emitted eagerly via nodeSignals$ — complete it
      // so no more emissions happen, then continue with error handling.
      nodeSignals$.complete();
      const accumulated: Signal[] = [];

      // Abort
      if (err instanceof Aborted) {
        return merge(
          from(accumulated),
          of(emit(nodeCtxId, nodeKey, { type: "node:error", error: "aborted" })),
          ...pending,
          defer((): Observable<Signal> => { throw err; }),
        );
      }

      // Interrupts from policy — route to fallback handler
      if (err instanceof Interrupt) {
        return resolveInterrupt$(err, nodeKey, nodeCtxId).pipe(
          concatMap((ir): Observable<Signal> => {
            if (ir.decision === "retry" || ir.decision === "mutate") {
              return merge(
                from(accumulated),
                from(ir.signals),
                executeNode$(ctx, nodeKey, input, scope).pipe(
                  catchError((retryErr) => {
                    if (retryErr instanceof Interrupt) {
                      return merge(
                        of(emit(nodeCtxId, nodeKey, { type: "oracle:interrupt:in", payload: retryErr.payload })),
                        of(emit(nodeCtxId, nodeKey, { type: "oracle:interrupt:out", payload: retryErr.payload, decision: "deny:retry-failed" })),
                        of(emit(nodeCtxId, nodeKey, { type: "node:error", error: "interrupt:retry failed — condition unchanged" })),
                        ...pending,
                      );
                    }
                    throw retryErr;
                  }),
                ),
              );
            }
            if (ir.decision === "deny") {
              return merge(
                from(accumulated),
                from(ir.signals),
                of(emit(nodeCtxId, nodeKey, { type: "node:error", error: `interrupt:denied (${ir.reason})` })),
                ...pending,
              );
            }
            return merge(
              from(accumulated),
              from(ir.signals),
              of(emit(nodeCtxId, nodeKey, { type: "node:after", output: JSON.stringify(ir.value) })),
              ...pending,
            );
          }),
        );
      }

      // PolicyExceeded — non-mutable limits end up here
      if (err instanceof PolicyExceeded) {
        return merge(
          from(accumulated),
          of(emit(nodeCtxId, nodeKey, { type: "node:error", error: `limit:${err.label} (${err.value}/${err.limit})` })),
          ...pending,
        );
      }

      // Denied
      if (err instanceof Denied) {
        return merge(
          from(accumulated),
          of(emit(nodeCtxId, nodeKey, { type: "node:error", error: `denied:${err.oracle} (${err.reason})` })),
          ...pending,
        );
      }

      return merge(
        from(accumulated),
        of(emit(nodeCtxId, nodeKey, { type: "node:error", error: String(err) })),
        ...pending,
      );
    }),
  );
}

// ═══════════════════════════════════════════════════════════
// classifyEdges — back-edge detection for cycle support
// ═══════════════════════════════════════════════════════════

interface EdgeClassification {
  forward: Array<{ from: string; to: string }>;
  back: Array<{ from: string; to: string }>;
  cyclicNodes: Set<string>;
}

function kahnOrder(
  nodes: readonly string[],
  edges: ReadonlyArray<{ from: string; to: string }>,
): string[] {
  const inDegree = new Map<string, number>();
  const adj = new Map<string, string[]>();
  for (const n of nodes) { inDegree.set(n, 0); adj.set(n, []); }
  for (const e of edges) {
    inDegree.set(e.to, (inDegree.get(e.to) ?? 0) + 1);
    adj.get(e.from)?.push(e.to);
  }
  const order: string[] = [];
  const queue = nodes.filter((n) => inDegree.get(n) === 0);
  let head = 0;
  while (head < queue.length) {
    const n = queue[head++]!;
    order.push(n);
    for (const next of adj.get(n) ?? []) {
      const d = inDegree.get(next)! - 1;
      inDegree.set(next, d);
      if (d === 0) queue.push(next);
    }
  }
  return order;
}

/** Find all nodes that participate in cycles using Tarjan's SCC algorithm. O(V+E). */
function findCyclicNodes(
  nodes: readonly string[],
  edges: ReadonlyArray<{ from: string; to: string }>,
): Set<string> {
  const adj = new Map<string, string[]>();
  for (const n of nodes) adj.set(n, []);
  for (const e of edges) adj.get(e.from)?.push(e.to);

  let index = 0;
  const indices = new Map<string, number>();
  const lowlinks = new Map<string, number>();
  const onStack = new Set<string>();
  const stack: string[] = [];
  const cyclic = new Set<string>();

  function strongconnect(v: string): void {
    indices.set(v, index);
    lowlinks.set(v, index);
    index++;
    stack.push(v);
    onStack.add(v);

    for (const w of adj.get(v) ?? []) {
      if (!indices.has(w)) {
        strongconnect(w);
        lowlinks.set(v, Math.min(lowlinks.get(v)!, lowlinks.get(w)!));
      } else if (onStack.has(w)) {
        lowlinks.set(v, Math.min(lowlinks.get(v)!, indices.get(w)!));
      }
    }

    if (lowlinks.get(v) === indices.get(v)) {
      const scc: string[] = [];
      let w: string;
      do {
        w = stack.pop()!;
        onStack.delete(w);
        scc.push(w);
      } while (w !== v);
      // Only SCCs with more than one node, or self-loops, are cyclic
      if (scc.length > 1) {
        for (const n of scc) cyclic.add(n);
      } else if (scc.length === 1 && (adj.get(scc[0]!) ?? []).includes(scc[0]!)) {
        cyclic.add(scc[0]!);
      }
    }
  }

  for (const n of nodes) {
    if (!indices.has(n)) strongconnect(n);
  }
  return cyclic;
}

function classifyEdges(
  nodes: readonly string[],
  edges: ReadonlyArray<{ from: string; to: string }>,
): EdgeClassification {
  const order = kahnOrder(nodes, edges);
  const ordered = new Set(order);
  const stuckNodes = nodes.filter((n) => !ordered.has(n));

  if (stuckNodes.length === 0) {
    const pos = new Map(order.map((n, i) => [n, i]));
    const forward: Array<{ from: string; to: string }> = [];
    const back: Array<{ from: string; to: string }> = [];
    for (const e of edges) {
      const fp = pos.get(e.from);
      const tp = pos.get(e.to);
      if (fp !== undefined && tp !== undefined && fp < tp) forward.push(e);
      else back.push(e);
    }
    return { forward, back, cyclicNodes: new Set() };
  }

  const stuckSet = new Set(stuckNodes);
  const cyclicNodes = findCyclicNodes(stuckNodes, edges.filter((e) =>
    stuckSet.has(e.from) && stuckSet.has(e.to),
  ));

  const downstreamNodes = stuckNodes.filter((n) => !cyclicNodes.has(n));
  const downstreamSet = new Set(downstreamNodes);
  const downstreamOrder = kahnOrder(downstreamNodes, edges.filter((e) =>
    downstreamSet.has(e.from) && downstreamSet.has(e.to),
  ));
  const remainingDownstream = downstreamNodes.filter((n) => !downstreamOrder.includes(n));

  const fullOrder = [...order, ...downstreamOrder, ...remainingDownstream];
  const pos = new Map(fullOrder.map((n, i) => [n, i]));

  const forward: Array<{ from: string; to: string }> = [];
  const back: Array<{ from: string; to: string }> = [];
  for (const e of edges) {
    if (cyclicNodes.has(e.from) && !cyclicNodes.has(e.to)) { forward.push(e); continue; }
    const fp = pos.get(e.from);
    const tp = pos.get(e.to);
    if (fp !== undefined && tp !== undefined && fp < tp) forward.push(e);
    else back.push(e);
  }
  return { forward, back, cyclicNodes };
}

// ═══════════════════════════════════════════════════════════
// executeGraph$ — edge-driven DAG/DG scheduling
// ═══════════════════════════════════════════════════════════

function executeGraph$(
  ctx: GraphExecCtx,
  input: unknown,
): Observable<Signal> {
  const deduped: GraphExecCtx = ctx.topology.nodes.length !== new Set(ctx.topology.nodes).size
    ? { ...ctx, topology: { ...ctx.topology, nodes: [...new Set(ctx.topology.nodes)] } }
    : ctx;
  const topology = deduped.topology;

  const rootAgent = deduped.ancestry[deduped.ancestry.length - 1]!.nodeKey;

  // Reset graph-scoped counters for this graph
  ctx.policies.registerGraph(deduped.invocationId, deduped.graphId);
  ctx.policies.resetGraphWindow(deduped.graphId);

  // Filter edges to only those referencing nodes in the topology
  const nodeSet = new Set(topology.nodes);
  const validEdges = topology.edges.filter((e) => nodeSet.has(e.from) && nodeSet.has(e.to));

  const { cyclicNodes: cycleNodes } = classifyEdges(topology.nodes, validEdges);
  const hasCycles = cycleNodes.size > 0;

  const nodeOutputs = new Map<string, ReplaySubject<unknown>>();
  topology.nodes.forEach((n) => nodeOutputs.set(n, new ReplaySubject<unknown>(1)));

  // On abort, complete all pending subjects to unblock forkJoin
  if (ctx.signal) {
    const abortHandler = () => {
      for (const subject of nodeOutputs.values()) {
        if (!subject.closed) {
          subject.next({ error: "aborted" });
          subject.complete();
        }
      }
    };
    if (ctx.signal.aborted) {
      abortHandler();
    } else {
      ctx.signal.addEventListener("abort", abortHandler, { once: true });
    }
  }

  // Acyclic node streams
  const acyclicStreams = topology.nodes
    .filter((n) => !hasCycles || !cycleNodes.has(n))
    .map((nodeKey) => {
      const preds = validEdges.filter((e) => e.to === nodeKey).map((e) => e.from);

      const deps$ = preds.length
        ? forkJoin(preds.map((p) => nodeOutputs.get(p)!.pipe(first())))
        : of([input]);

      // map node — dynamic fan-out over upstream array
      const mapCfg = ctx.manifest.getMap?.(nodeKey) ?? null;
      if (mapCfg) {
        return deps$.pipe(
          concatMap((predOutputs) => {
            const upstream = (predOutputs[0] ?? input) as Record<string, unknown>;
            const items = upstream[mapCfg.items] as unknown[] | undefined;
            if (!items?.length) {
              const subject = nodeOutputs.get(nodeKey)!;
              subject.next({ items: [], count: 0 });
              subject.complete();
              return of(emit(deduped, nodeKey, { type: "node:after", output: { items: [], count: 0 } }));
            }

            const newId = resolveNewId(ctx.manifest);
            let completed = 0;
            const iterations$ = items.map((item, idx) => {
              const iterGraphId = `${deduped.graphId}:fe_${idx}`;
              const iterCtx: GraphExecCtx = {
                ...deduped,
                graphId: iterGraphId,
                topology: mapCfg.subTopology,
                ancestry: [
                  ...deduped.ancestry,
                  { nodeKey: `${nodeKey}[${idx}]`, nodeId: newId("node"), graphId: iterGraphId, oracleId: "", policyId: "" },
                ],
              };
              return executeGraph$(iterCtx, item).pipe(
                tap((signal) => {
                  // track completion of each iteration
                  if (signal.type === "graph:after") {
                    completed++;
                    if (completed === items.length) {
                      const subject = nodeOutputs.get(nodeKey)!;
                      subject.next({ items_count: items.length });
                      subject.complete();
                    }
                  }
                }),
              );
            });

            // stream signals as they happen — no buffering
            return merge(...iterations$);
          }),
          catchError((err) => {
            const subject = nodeOutputs.get(nodeKey)!;
            subject.next({ error: String(err) });
            subject.complete();
            if (err instanceof Aborted) throw err;
            return of(emit(deduped, nodeKey, { type: "node:error", error: String(err) }));
          }),
        );
      }

      return deps$.pipe(
        concatMap((predOutputs) => {
          return executeNode$(deduped, nodeKey, predOutputs);
        }),
        tap((signal) => {
          const subject = nodeOutputs.get(nodeKey)!;
          if (signal.type === "node:after") { subject.next(signal.output); subject.complete(); }
          if (signal.type === "node:error") { subject.next({ error: signal.error }); subject.complete(); }
        }),
        catchError((err) => {
          const subject = nodeOutputs.get(nodeKey)!;
          subject.next({ error: String(err) });
          subject.complete();
          if (err instanceof Aborted) throw err;
          return of(emit(deduped, nodeKey, { type: "node:error", error: String(err) }));
        }),
      );
    });

  // Cycle scheduling
  const cycleStream$ = !hasCycles ? EMPTY : defer(() => {
    const cycleNodeList = topology.nodes.filter((n) => cycleNodes.has(n));
    const cycleOutputs = new Map<string, unknown>();

    const acyclicPreds = new Set<string>();
    for (const nk of cycleNodeList) {
      for (const e of validEdges) {
        if (e.to === nk && !cycleNodes.has(e.from)) acyclicPreds.add(e.from);
      }
    }

    const seed$ = acyclicPreds.size > 0
      ? forkJoin(
          [...acyclicPreds].map((p) => nodeOutputs.get(p)!.pipe(first())),
        ).pipe(
          concatMap((outputs) => {
            [...acyclicPreds].forEach((p, i) => cycleOutputs.set(p, outputs[i]));
            return EMPTY;
          }),
        )
      : EMPTY;

    function cycleRound$(round: number): Observable<Signal> {
      const runnable = cycleNodeList.filter((nk) => {
        if (round === 0) return true;

        const action: PolicyAction = {
          node: nk,
          oracle: "",
          input: undefined,
          history: [],
          ancestry: (() => {
            const a = [...deduped.ancestry];
            const exec: AncestryEntry = { nodeKey: nk, nodeId: deduped.nodeId, graphId: deduped.graphId, oracleId: deduped.oracleId, policyId: deduped.policyId };
            for (let i = 0; i < round; i++) a.push(exec);
            return a;
          })(),
          target: nk,
          timestamp: Date.now(),
          invocationId: deduped.invocationId,
          graphId: deduped.graphId,
          nodeId: "",
          oracleId: "",
          policyId: "",
        };

        const nodePolicies = ctx.manifest.getNodePolicies?.(nk) ?? [];
        const rootAgent = deduped.ancestry.length > 0 ? deduped.ancestry[0]!.nodeKey : nk;
        const graphPolicies = ctx.manifest.getGraphPolicies?.(rootAgent) ?? [];
        const globalPolicies = [...ctx.globalPolicies];
        const allPolicies = [...nodePolicies, ...graphPolicies, ...globalPolicies];

        for (const p of allPolicies) {
          if (!isCustomPolicy(p)) continue;
          const r = p.policy(action);
          if (r.approval === "deny") return false;
          if (r.approval === "allow" && "stop" in r && r.stop) return true;
        }
        return true;
      });

      if (runnable.length === 0) {
        for (const nk of cycleNodeList) {
          const subject = nodeOutputs.get(nk)!;
          const output = cycleOutputs.get(nk);
          if (output) { subject.next(output); }
          subject.complete();
        }
        return EMPTY;
      }

      const roundStreams = runnable.map((nk) => {
        const preds = validEdges.filter((e) => e.to === nk).map((e) => e.from);
        const predOutputs: unknown[] = [];
        for (const p of preds) {
          const output = cycleOutputs.get(p);
          if (output !== undefined) predOutputs.push(output);
        }
        const edgeInput = predOutputs.length > 0 ? predOutputs : [input];

        const iterAncestry = [...deduped.ancestry];
        const exec: AncestryEntry = { nodeKey: nk, nodeId: deduped.nodeId, graphId: deduped.graphId, oracleId: deduped.oracleId, policyId: deduped.policyId };
        for (let i = 0; i < round; i++) iterAncestry.push(exec);
        const iterCtx = { ...deduped, ancestry: [...iterAncestry, exec] };

        return executeNode$(iterCtx, nk, edgeInput).pipe(
          tap((signal) => {
            if (signal.type === "node:after") cycleOutputs.set(nk, signal.output);
            if (signal.type === "node:error") cycleOutputs.set(nk, { error: signal.error });
          }),
          catchError((err) => {
            if (err instanceof Aborted) throw err;
            cycleOutputs.set(nk, { error: String(err) });
            return of(emit(deduped, nk, { type: "node:error", error: String(err) }));
          }),
        );
      });

      return merge(...roundStreams).pipe(
        toArray(),
        concatMap((sigs) => concat(from(sigs), cycleRound$(round + 1))),
      );
    }

    return concat(seed$, cycleRound$(0));
  });

  const nodeStreams = [...acyclicStreams, cycleStream$];

  let graphAfterEmitted = false;

  return merge(
    of(emit(deduped, rootAgent, { type: "graph:before", key: rootAgent, nodeCount: topology.nodes.length })),
    merge(...nodeStreams),
  ).pipe(
    catchError((err) => {
      graphAfterEmitted = true;
      return merge(
        of(emit(deduped, rootAgent, { type: "graph:error", key: rootAgent, error: String(err) })),
        of(emit(deduped, rootAgent, { type: "graph:after", key: rootAgent })),
        defer((): Observable<Signal> => { throw err; }),
      );
    }),
    (source$) => concat(source$, defer(() => {
      if (graphAfterEmitted) return EMPTY;
      return of(emit(deduped, rootAgent, { type: "graph:after", key: rootAgent }));
    })),
  );
}

// ═══════════════════════════════════════════════════════════
// GRAPH FACTORY
// ═══════════════════════════════════════════════════════════

export interface EngineDef {
  /** Oracle definitions — hierarchical, policy-driven. Keys are opaque strings with `:` hierarchy. */
  oracles: Record<string, Oracle>;
  /** Global policies — checked last in bubble order. */
  policies?: CustomPolicy[];
  /** Fallback interrupt handler. Called when consumer doesn't catch Interrupt in execute(). */
  handleInterrupt?: (interrupt: Interrupt, ctx: InterruptCtx) => Promise<InterruptResult>;
  /** Timeout in ms for handleInterrupt calls. Default: 120_000 (2 minutes). */
  interruptTimeout?: number;
}

/** Options for a top-level `Engine.exec$` call. */
export interface ExecOpts {
  /** Abort signal for the whole exec tree. */
  signal?: AbortSignal;
  /** Session context — propagated to every NodeExec in the tree. */
  session?: { id: string; origin: string };
}

export interface Engine {
  /** Execute a graph. All accounting flows through signals. */
  exec$(manifest: Manifest, key: string, input: unknown, opts?: ExecOpts): Observable<Signal>;

  /** Snapshot all policy counters for persistence. */
  snapshot(): PolicySnapshot;

  /** Restore policy counters from a previous snapshot (e.g. after server restart). */
  restore(snapshot: PolicySnapshot): void;
}

/** Default ID generator used when Manifest.newId is not provided. */
function makeDefaultNewId(): (entity: string) => string {
  let counter = 0;
  return (entity: string) => `${entity.slice(0, 2)}_${++counter}`;
}

/** Cache for default ID generators keyed by manifest instance. */
const defaultNewIdCache = new WeakMap<Manifest, (entity: string) => string>();

/** Resolve the newId function from the manifest, falling back to internal default. */
function resolveNewId(manifest: Manifest): (entity: "manifest" | "graph" | "node" | "oracle" | "policy") => string {
  if (manifest.newId) return manifest.newId;
  let cached = defaultNewIdCache.get(manifest);
  if (!cached) {
    cached = makeDefaultNewId();
    defaultNewIdCache.set(manifest, cached);
  }
  return cached;
}

export function createEngine(config: EngineDef): Engine {
  const oracles = config.oracles;
  const globalPolicies = config.policies ?? [];
  const handleInterrupt = config.handleInterrupt;
  const interruptTimeout = config.interruptTimeout ?? 120_000;
  const policies = new PolicyState();

  // Warn at init: LLM oracles without rounds policy = unbounded oracle.llm() loop
  for (const [key, oracle] of Object.entries(oracles)) {
    if (key === "llm" || key.startsWith("llm:")) {
      const hasRounds = oracle.policies.some((p) => isOraclePolicy(p) && p.type === "rounds");
      if (!hasRounds) {
        console.warn(`⚠ oracle "${key}" has no "rounds" policy — oracle.llm() will loop until LLM stops returning tool calls`);
      }
    }
  }

  return {
    exec$(manifest, key, message, opts) {
      const newId = resolveNewId(manifest);
      const invocationId = newId("manifest");
      const graphId = newId("graph");

      policies.resetInvocationWindow(invocationId);

      const ctx: GraphExecCtx = {
        invocationId,
        graphId,
        nodeId: "",
        oracleId: "",
        policyId: "",
        manifest,
        topology: manifest.getGraph(key),
        ancestry: [{ nodeKey: key, nodeId: "", graphId, oracleId: "", policyId: "" }],
        oracles,
        globalPolicies,
        handleInterrupt,
        interruptTimeout,
        signal: opts?.signal,
        policies,
        nodeState: new Map(),
        sessionId: opts?.session?.id ?? null,
        sessionOrigin: opts?.session?.origin ?? null,
      };

      return executeGraph$(ctx, message).pipe(
        finalize(() => policies.cleanupInvocation(invocationId)),
      );
    },

    snapshot() {
      return policies.snapshot();
    },

    restore(snapshot) {
      policies.restore(snapshot);
    },
  };
}
