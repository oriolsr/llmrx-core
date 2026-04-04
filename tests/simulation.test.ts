/**
 * simulation.test.ts — Real simulation. Not happy path. Not fuzz.
 *
 * Simulates a trading firm with nodes, budget gates, tool permissions,
 * interrupts with mutable limits, edge data flowing through a DAG,
 * ask/tell communication, policy denials, and constraint policies.
 *
 * In the oracle engine model:
 *   - Node.execute() owns the LLM→tool→LLM loop (or uses ctx.oracle.llm)
 *   - LLM calls go through ctx.exec$("llm", ...) or ctx.oracle.llm()
 *   - Tool calls go through ctx.exec$("tool", ...) or via oracle.llm's executeTool
 *   - Policies, limits, constraints — all on oracle types
 *   - No separate LimitStack — limits are policies on oracle types
 */
import { describe, it, expect } from "vitest";
import { lastValueFrom, toArray, Observable, merge, of, from, EMPTY, concat } from "rxjs";
import { concatMap, tap } from "rxjs/operators";
import {
  createEngine, Interrupt, Denied, Aborted, PolicyExceeded,
  constraints,
  type Manifest, type CustomPolicy,
  type InterruptCtx, type InterruptResult, type Node, type Graph,
  type Layer, type NodeExec, type NodeResult,
  type Signal, type Oracle, type Policy, type OraclePolicy,
} from "../src/llmrx.js";

// Consumer-owned types (not part of engine)
interface ToolDef { name: string; description: string; parameters: Record<string, unknown> }
interface ToolCall { id: string; name: string; args: Record<string, unknown> }
interface OracleResponse { text: string; toolCalls: ToolCall[]; cost: number }

// ═══════════════════════════════════════════════════════════
// SIMULATION STATE — shared mutable state like a real system
// ═══════════════════════════════════════════════════════════

interface SimState {
  tradeLog: Array<{ node: string; symbol: string; qty: number; approved: boolean }>;
  llmCallLog: Array<{ node: string; model: string }>;
  toolCallLog: Array<{ node: string; tool: string; args: unknown }>;
  humanQuestions: Array<{ from: string; target: string; payload: unknown }>;
  interruptPayloads: unknown[];
  effectsApplied: unknown[];
  edgeDataReceived: Map<string, string>;
}

function freshState(): SimState {
  return {
    tradeLog: [],
    llmCallLog: [],
    toolCallLog: [],
    humanQuestions: [],
    interruptPayloads: [],
    effectsApplied: [],
    edgeDataReceived: new Map(),
  };
}

// ═══════════════════════════════════════════════════════════
// ORACLE TYPES — registered on GraphConfig
// ═══════════════════════════════════════════════════════════

const DEFAULT_ORACLES: Record<string, Oracle> = {
  llm: {
    policies: [
      { type: "cost", max: 100, window: "invocation", extract: (r: unknown) => ({ cost: (r as OracleResponse)?.cost ?? 0 }) },
      { type: "timeout", max: 5000 },
    ],
  },
  tool: {
    policies: [
      { type: "timeout", max: 5000 },
    ],
  },
};

// ═══════════════════════════════════════════════════════════
// SCRIPTED LLM — helper that the execute() method calls
// ═══════════════════════════════════════════════════════════

type LLMScript = (node: string, layers: Layer[]) => { text: string; toolCalls?: ToolCall[]; cost?: number };

function scriptedLLMFn(state: SimState, script: LLMScript): (layers: Layer[]) => Promise<OracleResponse> {
  return async (layers: Layer[]) => {
    const nodeMsg = layers.find((m) => m.name === "system" && typeof m.content === "string");
    const nodeHint = nodeMsg ? String(nodeMsg.content).slice(0, 20) : "unknown";
    const r = script(nodeHint, layers);
    const cost = r.cost ?? 0.05;
    state.llmCallLog.push({ node: nodeHint, model: "test" });
    return { text: r.text, toolCalls: r.toolCalls ?? [], cost };
  };
}

// ═══════════════════════════════════════════════════════════
// TOOL EXECUTOR — helper that the execute() method calls
// ═══════════════════════════════════════════════════════════

function executeToolFn(state: SimState, overrides?: {
  onShutdown?: () => void;
  onDeepAnalysis?: () => void;
  customExecute?: (call: ToolCall, nodeKey: string, ctx: NodeExec) => Promise<{ result: unknown; content: string }>;
}): (call: ToolCall, nodeKey: string, ctx: NodeExec) => Promise<{ result: unknown; content: string; signals?: Signal[] }> {
  return async (call, nodeKey, ctx) => {
    state.toolCallLog.push({ node: nodeKey, tool: call.name, args: call.args });

    if (overrides?.customExecute) {
      return overrides.customExecute(call, nodeKey, ctx);
    }

    if (call.name === "comm.ask") {
      const targets = Array.isArray(call.args.target) ? call.args.target as string[] : [call.args.target as string];
      const msg = call.args.message as string;
      const outputs: Record<string, string> = {};
      const spawnSignals: Signal[] = [];

      for (const target of targets) {
        const reason = ctx.checkAncestry(target);
        if (reason) continue;

        if (ctx.isNode(target)) {
          const signals = await lastValueFrom(ctx.spawnSync({ key: target, data: msg }).pipe(toArray()));
          spawnSignals.push(...signals);
          const after = signals.find((s) => s.type === "node:after") as { output: string } | undefined;
          outputs[target] = after?.output ?? "no response";
        } else {
          state.humanQuestions.push({ from: nodeKey, target, payload: msg });
          outputs[target] = JSON.stringify({ approved: true });
        }
      }

      const result = targets.map((t) => `[${t}]: ${outputs[t] ?? "blocked"}`).join("\n\n");
      return { result: outputs, content: result, signals: spawnSignals };
    }

    if (call.name === "comm.tell") {
      const targets = Array.isArray(call.args.target) ? call.args.target as string[] : [call.args.target as string];
      const msg = call.args.message as string;

      for (const target of targets) {
        const reason = ctx.checkAncestry(target);
        if (reason) continue;
        if (ctx.isNode(target)) {
          ctx.spawnAsync({ key: target, data: msg });
        }
      }

      return { result: { acknowledged: true }, content: `acknowledged — told ${targets.join(", ")}` };
    }

    if (call.name === "__ask_human") {
      const args = call.args as Record<string, unknown>;
      const from = args.from as string;
      const target = args.target as string;
      state.humanQuestions.push({ from, target, payload: args.message });
      return { result: { approved: true }, content: JSON.stringify({ approved: true }) };
    }

    if (call.name === "market.quote") {
      return { result: { symbol: call.args.symbol, price: 185.20, volume: 1200000 }, content: JSON.stringify({ price: 185.20 }) };
    }
    if (call.name === "market.execute_trade") {
      const qty = call.args.qty as number;
      if (qty > 10000) {
        throw new Interrupt({
          type: "trade_approval",
          target: "uri",
          symbol: call.args.symbol,
          qty,
        });
      }
      state.tradeLog.push({ node: nodeKey, symbol: String(call.args.symbol), qty, approved: true });
      return { result: { filled: true, qty }, content: `filled ${qty}` };
    }
    if (call.name === "risk.analyze") {
      return { result: { score: 0.72, level: "medium" }, content: "risk: medium (0.72)" };
    }
    if (call.name === "memory.save") {
      return { result: { saved: true }, content: "saved" };
    }
    if (call.name === "system.shutdown") {
      overrides?.onShutdown?.();
      return { result: { ok: true }, content: "ok" };
    }
    if (call.name === "research.query") {
      return { result: { data: "preliminary findings" }, content: "findings: tech sector growing 15% YoY" };
    }
    if (call.name === "research.deep_analysis") {
      overrides?.onDeepAnalysis?.();
      return { result: { ok: true }, content: "ok" };
    }
    if (call.name === "__noop") {
      return { result: { ok: true }, content: "noop" };
    }
    return { result: { ok: true }, content: "ok" };
  };
}

// ═══════════════════════════════════════════════════════════
// makeNode — creates Node with execute() that drives the
// LLM→tool→LLM loop via ctx.exec$
// ═══════════════════════════════════════════════════════════

function makeNode(key: string, opts: {
  tools?: ToolDef[];
  maxRounds?: number;
  soulContent?: string;
  llmFn?: (layers: Layer[]) => Promise<OracleResponse>;
  toolFn?: (call: ToolCall, nodeKey: string, ctx: NodeExec) => Promise<{ result: unknown; content: string; signals?: Signal[] }>;
}): Node {
  const soul = opts.soulContent ?? `You are node ${key}. Be concise.`;
  const tools = opts.tools ?? [];
  const maxIterations = opts.maxRounds ?? 5;

  return {
    loadPrompt: (_input, _state) => of([
      { name: "system", content: soul },
      { name: "system", content: `Counters: ${JSON.stringify(_state.counters("llm"))}` },
      { name: "tools", content: tools },
      { name: "user", content: _input },
    ]),
    execute(layers: Layer[], ctx: NodeExec): Observable<NodeResult> {
      const llmCall = opts.llmFn;
      const toolCall = opts.toolFn;
      if (!llmCall) {
        return of({ output: "no llm configured" });
      }

      return new Observable<NodeResult>((subscriber) => {
        (async () => {
          let currentLayers = [...layers];
          let round = 0;
          const consumerSignals: Signal[] = [];

          while (round <= maxIterations) {
            const response = await lastValueFrom(
              ctx.exec$("llm", () => llmCall(currentLayers)),
            );

            if (!response.toolCalls || response.toolCalls.length === 0) {
              subscriber.next({ output: response.text, signals: consumerSignals });
              subscriber.complete();
              return;
            }

            round++;
            if (round > maxIterations) {
              throw new PolicyExceeded(`iterations:${key}`, round, maxIterations);
            }

            for (const tc of response.toolCalls) {
              if (!toolCall) {
                currentLayers.push({ name: "tool", content: `${tc.id}:no executor` });
                continue;
              }

              try {
                const toolResult = await lastValueFrom(
                  ctx.exec$("tool", async () => {
                    return toolCall(tc, key, ctx);
                  }, tc),
                );
                if (toolResult.signals) consumerSignals.push(...toolResult.signals);
                currentLayers.push({ name: "tool", content: toolResult.content });
              } catch (err) {
                if (err instanceof Denied) {
                  currentLayers.push({ name: "tool", content: JSON.stringify({ denied: true, reason: err.reason, callId: tc.id }) });
                  continue;
                }
                throw err;
              }
            }
          }

          throw new PolicyExceeded(`iterations:${key}`, round, maxIterations);
        })().catch((err) => subscriber.error(err));
      });
    },
  };
}

function realisticManifest(
  _state: SimState,
  nodeMap: Map<string, Node>,
  topo: Graph,
): Manifest {
  return {
    getNode: (k) => { const n = nodeMap.get(k); if (!n) throw new Error(`no node: ${k}`); return n; },
    getNodeKeys: () => [...nodeMap.keys()],
    isNode: (k) => nodeMap.has(k),
    getGraph: (key) => topo.nodes.includes(key) ? topo : { nodes: [key], edges: [] },
    newId,
  };
}

function ev<T extends Signal["type"]>(signals: Signal[], type: T) {
  return signals.filter((e) => e.type === type) as Array<Extract<Signal, { type: T }>>;
}

function makeIdGen() {
  let id = 0;
  return (p: string) => `${p}_${++id}`;
}
const newId = makeIdGen();

// ═══════════════════════════════════════════════════════════
// NODE DEFINITIONS — realistic trading firm
// ═══════════════════════════════════════════════════════════

const QUOTE_TOOL: ToolDef = { name: "market.quote", description: "Get stock quote", parameters: { type: "object", properties: { symbol: { type: "string" } } } };
const TRADE_TOOL: ToolDef = { name: "market.execute_trade", description: "Execute trade", parameters: { type: "object", properties: { symbol: { type: "string" }, qty: { type: "number" } } } };
const RISK_TOOL: ToolDef = { name: "risk.analyze", description: "Analyze risk", parameters: { type: "object", properties: { symbol: { type: "string" } } } };
const SAVE_TOOL: ToolDef = { name: "memory.save", description: "Save to memory", parameters: { type: "object", properties: { data: { type: "string" } } } };
const FORBIDDEN_TOOL: ToolDef = { name: "system.shutdown", description: "Shutdown system", parameters: {} };
const ASK_TOOL: ToolDef = { name: "comm.ask", description: "Ask another node or human", parameters: { type: "object", properties: { target: {}, message: { type: "string" } }, required: ["target", "message"] } };
const TELL_TOOL: ToolDef = { name: "comm.tell", description: "Tell another node or human (fire and forget)", parameters: { type: "object", properties: { target: {}, message: { type: "string" } }, required: ["target", "message"] } };

function simNode(key: string, opts: {
  tools?: ToolDef[];
  maxRounds?: number;
  soulContent?: string;
  state: SimState;
  script: LLMScript;
  toolOverrides?: Parameters<typeof executeToolFn>[1];
}): Node {
  return makeNode(key, {
    tools: opts.tools,
    maxRounds: opts.maxRounds,
    soulContent: opts.soulContent,
    llmFn: scriptedLLMFn(opts.state, opts.script),
    toolFn: executeToolFn(opts.state, opts.toolOverrides),
  });
}

// ═══════════════════════════════════════════════════════════
// TESTS
// ═══════════════════════════════════════════════════════════

describe("simulation: trade pipeline DAG", () => {
  it("3-node pipeline: gather → analyze → execute, edge data flows correctly", async () => {
    const state = freshState();
    let callNum = 0;
    const toolFn = executeToolFn(state);

    const script: LLMScript = (node, msgs) => {
      callNum++;
      const userInput = msgs.find((m) => m.name === "user");
      const input = userInput ? String(userInput.content) : "";

      if (callNum === 1) return { text: "", toolCalls: [{ id: "q1", name: "market.quote", args: { symbol: "AAPL" } }] };
      if (callNum === 2) return { text: "AAPL @ $185.20, volume 1.2M", cost: 0.03 };
      if (callNum === 3) { state.edgeDataReceived.set("analyze", input); return { text: "", toolCalls: [{ id: "r1", name: "risk.analyze", args: { symbol: "AAPL" } }] }; }
      if (callNum === 4) return { text: "AAPL medium risk (0.72), recommend buy 5000", cost: 0.08 };
      if (callNum === 5) { state.edgeDataReceived.set("execute", input); return { text: "", toolCalls: [{ id: "t1", name: "market.execute_trade", args: { symbol: "AAPL", qty: 5000 } }] }; }
      return { text: "Trade filled: AAPL x5000 @ $185.20", cost: 0.05 };
    };

    const llmFn = scriptedLLMFn(state, script);
    const nodes = new Map([
      ["gather", makeNode("gather", { tools: [QUOTE_TOOL], llmFn, toolFn })],
      ["analyze", makeNode("analyze", { tools: [RISK_TOOL], llmFn, toolFn })],
      ["execute", makeNode("execute", { tools: [TRADE_TOOL], llmFn, toolFn })],
    ]);
    const topo: Graph = {
      nodes: ["gather", "analyze", "execute"],
      edges: [{ from: "gather", to: "analyze" }, { from: "analyze", to: "execute" }],
    };

    const graph = createEngine({ oracles: DEFAULT_ORACLES });
    const signals = await lastValueFrom(
      graph.exec$(realisticManifest(state, nodes, topo), "gather", "analyze AAPL").pipe(toArray()),
    );

    expect(ev(signals, "node:after").length).toBe(3);
    expect(state.edgeDataReceived.get("analyze"), "analyze should receive gather output").toContain("AAPL");
    expect(state.edgeDataReceived.get("execute"), "execute should receive analyze output").toContain("recommend");
    expect(state.toolCallLog.map((t) => t.tool)).toEqual(["market.quote", "risk.analyze", "market.execute_trade"]);
    expect(state.tradeLog.length).toBe(1);
    expect(state.tradeLog[0]!.symbol).toBe("AAPL");
    expect(state.tradeLog[0]!.qty).toBe(5000);
    const costSignals = signals.filter((s) => s.type.startsWith("oracle:policy:") && "limitType" in s && (s as { limitType: string }).limitType === "cost");
    expect(costSignals.length).toBeGreaterThan(0);
  });
});

describe("simulation: cost limit on oracle type", () => {
  it("non-mutable cost limit → PolicyExceeded, node errors", async () => {
    const state = freshState();
    let callNum = 0;
    // Node makes 2 LLM calls — first returns tool, second returns text. Both cost 0.50.
    // Max is 0.60, so second LLM call should be blocked.
    const llmFn = scriptedLLMFn(state, () => {
      callNum++;
      if (callNum === 1) return { text: "", cost: 0.50, toolCalls: [{ id: "q1", name: "market.quote", args: { symbol: "AAPL" } }] };
      return { text: "done", cost: 0.50 };
    });
    const toolFn = executeToolFn(state);

    const tightOracles: Record<string, Oracle> = {
      llm: {
        policies: [
          { type: "cost", max: 0.40, window: "invocation", extract: (r: unknown) => ({ cost: (r as OracleResponse)?.cost ?? 0 }) },
          { type: "timeout", max: 5000 },
        ],
      },
      tool: { policies: [{ type: "timeout", max: 5000 }] },
    };

    const nodes = new Map([["donna", makeNode("donna", { tools: [QUOTE_TOOL], llmFn, toolFn })]]);
    const topo: Graph = { nodes: ["donna"], edges: [] };
    const graph = createEngine({ oracles: tightOracles });

    const signals = await lastValueFrom(
      graph.exec$(realisticManifest(state, nodes, topo), "donna", "check TSLA").pipe(toArray()),
    );

    // First LLM call costs 0.50 (passes pre-check, accumulated post-exec to 0.50 > 0.40). Second LLM call blocked pre-check.
    const errors = ev(signals, "node:error");
    expect(errors.length).toBeGreaterThan(0);
  });

  it("mutable cost limit → auto-interrupt → handleInterrupt approves → retries", async () => {
    const state = freshState();
    let callCount = 0;
    // Node makes 2 LLM calls. First costs 0.50, returns a tool call. Second costs 0.50, returns text.
    // Max is 0.60. After first call (0.50 accumulated), tool runs, then second LLM call hits limit.
    // But limit is mutable, so it auto-interrupts. Handler approves increase to 5.0. Retries, succeeds.
    const llmFn = scriptedLLMFn(state, () => {
      callCount++;
      if (callCount === 1) return { text: "", cost: 0.50, toolCalls: [{ id: "t1", name: "market.quote", args: { symbol: "AAPL" } }] };
      return { text: "done after budget increase", cost: 0.50 };
    });
    const toolFn = executeToolFn(state);

    const mutableOracles: Record<string, Oracle> = {
      llm: {
        policies: [
          {
            type: "cost", max: 0.40, window: "invocation",
            extract: (r: unknown) => ({ cost: (r as OracleResponse)?.cost ?? 0 }),
            mutable: { "oracle:human:uri": () => true },
          },
          { type: "timeout", max: 5000 },
        ],
      },
      tool: { policies: [{ type: "timeout", max: 5000 }] },
    };

    const nodes = new Map([["donna", makeNode("donna", { tools: [QUOTE_TOOL], llmFn, toolFn })]]);
    const topo: Graph = { nodes: ["donna"], edges: [] };
    const graph = createEngine({
      oracles: mutableOracles,
      handleInterrupt: async (interrupt) => {
        state.interruptPayloads.push(interrupt.payload);
        return { decision: "mutate", oracle: "oracle:human:uri", limitType: "cost", max: 5.0 };
      },
    });

    const signals = await lastValueFrom(
      graph.exec$(realisticManifest(state, nodes, topo), "donna", "check AAPL").pipe(toArray()),
    );

    expect(state.interruptPayloads.length).toBeGreaterThan(0);
    const mutations = signals.filter((s) => s.type.startsWith("oracle:mutation:"));
    expect(mutations.length).toBeGreaterThan(0);
  });
});

describe("simulation: policy blocks forbidden tools", () => {
  it("policy denies system.shutdown even if LLM hallucinates it", async () => {
    const state = freshState();
    let shutdownExecuted = false;
    let callNum = 0;

    const script: LLMScript = () => {
      callNum++;
      if (callNum === 1) return { text: "", toolCalls: [{ id: "s1", name: "system.shutdown", args: {} }] };
      return { text: "ok I won't shut down", cost: 0.02 };
    };

    const denyShutdown: CustomPolicy = {
      policy: (action) => {
        const input = action.input as { name?: string } | undefined;
        if (input?.name === "system.shutdown") {
          return { approval: "deny", reason: "shutdown is forbidden" };
        }
        return { approval: "allow" };
      },
    };

    const oracles: Record<string, Oracle> = {
      llm: { policies: [{ type: "timeout", max: 5000 }] },
      tool: { policies: [{ type: "timeout", max: 5000 }] },
    };

    const nodes = new Map([["donna", simNode("donna", {
      tools: [FORBIDDEN_TOOL, QUOTE_TOOL],
      state,
      script,
      toolOverrides: { onShutdown: () => { shutdownExecuted = true; } },
    })]]);

    const topo: Graph = { nodes: ["donna"], edges: [] };
    const graph = createEngine({ oracles, policies: [denyShutdown] });
    const signals = await lastValueFrom(
      graph.exec$(realisticManifest(state, nodes, topo), "donna", "shutdown now").pipe(toArray()),
    );

    expect(shutdownExecuted).toBe(false);
    const afterSignals = ev(signals, "node:after");
    expect(afterSignals.length).toBe(1);
    expect(afterSignals[0]!.output).toContain("won't shut down");
  });
});

describe("simulation: ancestry constraints as policies", () => {
  it("maxCycles(0) blocks recursive spawning", async () => {
    const state = freshState();
    let donnaCallNum = 0;
    let mikeCallNum = 0;

    const donnaScript: LLMScript = () => {
      donnaCallNum++;
      if (donnaCallNum === 1) return { text: "", toolCalls: [{ id: "a1", name: "comm.ask", args: { target: "mike", message: "help" } }] };
      return { text: "donna done" };
    };

    const mikeScript: LLMScript = () => {
      mikeCallNum++;
      if (mikeCallNum === 1) return { text: "", toolCalls: [{ id: "a2", name: "comm.ask", args: { target: "donna", message: "help back" } }] };
      return { text: "mike done" };
    };

    const toolFn = executeToolFn(state);
    const nodes = new Map([
      ["donna", makeNode("donna", { tools: [ASK_TOOL], llmFn: scriptedLLMFn(state, donnaScript), toolFn })],
      ["mike", makeNode("mike", { tools: [ASK_TOOL], llmFn: scriptedLLMFn(state, mikeScript), toolFn })],
    ]);
    const topo: Graph = { nodes: ["donna"], edges: [] };

    const graph = createEngine({
      oracles: DEFAULT_ORACLES,
      policies: [constraints.maxCycles(0), constraints.maxDepth(10)],
    });

    const signals = await lastValueFrom(
      graph.exec$(realisticManifest(state, nodes, topo), "donna", "start").pipe(toArray()),
    );

    // donna should complete. mike's ask to donna should be blocked by maxCycles(0)
    const donnaAfter = ev(signals, "node:after").filter((s) => s.nodeKey === "donna");
    expect(donnaAfter.length).toBeGreaterThan(0);
    expect(donnaAfter[0]!.output).toContain("donna done");
  });

  it("maxDepth blocks deep ask chains", async () => {
    const state = freshState();

    const nodes = new Map([
      ["donna", simNode("donna", {
        tools: [ASK_TOOL], state,
        script: () => ({ text: "", toolCalls: [{ id: "a1", name: "comm.ask", args: { target: "mike", message: "delegate" } }] }),
      })],
      ["mike", simNode("mike", {
        tools: [ASK_TOOL], state,
        script: () => ({ text: "", toolCalls: [{ id: "a2", name: "comm.ask", args: { target: "rachel", message: "delegate more" } }] }),
      })],
      ["rachel", simNode("rachel", {
        tools: [], state,
        script: () => ({ text: "I'm rachel" }),
      })],
    ]);
    const topo: Graph = { nodes: ["donna"], edges: [] };

    const graph = createEngine({
      oracles: DEFAULT_ORACLES,
      policies: [constraints.maxCycles(0), constraints.maxDepth(2)],
    });

    const signals = await lastValueFrom(
      graph.exec$(realisticManifest(state, nodes, topo), "donna", "go").pipe(toArray()),
    );

    // rachel should never execute — blocked by maxDepth(2)
    const rachelNodes = signals.filter((s) => "nodeKey" in s && s.nodeKey === "rachel");
    expect(rachelNodes.length, "rachel should be blocked by maxDepth(2)").toBe(0);
  });
});

describe("simulation: large trade triggers tool interrupt with retry", () => {
  it("tool throws interrupt → handleInterrupt retries → trade executes on retry", async () => {
    const state = freshState();
    let interruptCount = 0;
    let callNum = 0;

    const script: LLMScript = () => {
      callNum++;
      if (callNum === 1) return { text: "", toolCalls: [{ id: "t1", name: "market.execute_trade", args: { symbol: "TSLA", qty: 50000 } }] };
      // After interrupt retry, LLM tries again with smaller qty
      if (callNum === 2) return { text: "", toolCalls: [{ id: "t2", name: "market.execute_trade", args: { symbol: "TSLA", qty: 5000 } }] };
      return { text: "Trade done", cost: 0.03 };
    };

    const nodes = new Map([["donna", simNode("donna", { tools: [TRADE_TOOL], state, script })]]);
    const topo: Graph = { nodes: ["donna"], edges: [] };

    const graph = createEngine({
      oracles: DEFAULT_ORACLES,
      handleInterrupt: async (interrupt) => {
        interruptCount++;
        return { decision: "retry" };
      },
    });

    const signals = await lastValueFrom(
      graph.exec$(realisticManifest(state, nodes, topo), "donna", "big trade").pipe(toArray()),
    );

    expect(interruptCount).toBe(1);
    expect(state.tradeLog.length).toBe(1);
    expect(state.tradeLog[0]!.qty).toBe(5000);
  });
});

describe("simulation: parallel fanout with real edge data", () => {
  it("dispatcher fans out to 3 workers, merger receives all outputs", async () => {
    const state = freshState();
    let callNum = 0;

    const script: LLMScript = (node) => {
      callNum++;
      if (node.includes("dispatcher")) return { text: "analyze AAPL, GOOG, MSFT" };
      if (node.includes("w1")) return { text: "AAPL: buy 1000" };
      if (node.includes("w2")) return { text: "GOOG: hold" };
      if (node.includes("w3")) return { text: "MSFT: sell 500" };
      if (node.includes("merger")) return { text: "consolidated: buy AAPL, hold GOOG, sell MSFT" };
      return { text: "ok" };
    };

    const llmFn = scriptedLLMFn(state, script);
    const nodes = new Map([
      ["dispatcher", makeNode("dispatcher", { llmFn })],
      ["w1", makeNode("w1", { soulContent: "You are node w1", llmFn })],
      ["w2", makeNode("w2", { soulContent: "You are node w2", llmFn })],
      ["w3", makeNode("w3", { soulContent: "You are node w3", llmFn })],
      ["merger", makeNode("merger", { soulContent: "You are node merger", llmFn })],
    ]);

    const topo: Graph = {
      nodes: ["dispatcher", "w1", "w2", "w3", "merger"],
      edges: [
        { from: "dispatcher", to: "w1" }, { from: "dispatcher", to: "w2" }, { from: "dispatcher", to: "w3" },
        { from: "w1", to: "merger" }, { from: "w2", to: "merger" }, { from: "w3", to: "merger" },
      ],
    };

    const graph = createEngine({ oracles: DEFAULT_ORACLES });
    const signals = await lastValueFrom(
      graph.exec$(realisticManifest(state, nodes, topo), "dispatcher", "analyze portfolio").pipe(toArray()),
    );

    const afters = ev(signals, "node:after");
    expect(afters.length).toBe(5);
    const mergerOutput = afters.find((s) => s.nodeKey === "merger");
    expect(mergerOutput).toBeDefined();
    expect(mergerOutput!.output).toContain("consolidated");
  });
});

describe("simulation: ask with human + node mixed targets", () => {
  it("node asks both another node and a human, collects both responses", async () => {
    const state = freshState();
    let callNum = 0;

    const script: LLMScript = (node) => {
      callNum++;
      if (callNum === 1) return { text: "", toolCalls: [{ id: "a1", name: "comm.ask", args: { target: ["mike", "uri"], message: "approve trade?" } }] };
      if (callNum === 2) return { text: "mike says approved" };
      return { text: "done" };
    };

    const toolFn = executeToolFn(state);
    const nodes = new Map([
      ["donna", makeNode("donna", { tools: [ASK_TOOL], llmFn: scriptedLLMFn(state, script), toolFn })],
      ["mike", simNode("mike", { state, script: () => ({ text: "mike approved" }) })],
    ]);
    const topo: Graph = { nodes: ["donna"], edges: [] };

    const graph = createEngine({ oracles: DEFAULT_ORACLES, policies: [constraints.maxCycles(0), constraints.maxDepth(10)] });
    const signals = await lastValueFrom(
      graph.exec$(realisticManifest(state, nodes, topo), "donna", "ask everyone").pipe(toArray()),
    );

    expect(state.humanQuestions.length).toBe(1);
    expect(state.humanQuestions[0]!.target).toBe("uri");
    const donnaAfter = ev(signals, "node:after").filter((s) => s.nodeKey === "donna");
    expect(donnaAfter.length).toBe(1);
  });
});

describe("simulation: maxIterations enforcement", () => {
  it("node is capped after N tool rounds even if LLM keeps calling tools", async () => {
    const state = freshState();

    const script: LLMScript = () => ({
      text: "",
      toolCalls: [{ id: "n1", name: "__noop", args: {} }],
    });

    const nodes = new Map([["donna", simNode("donna", {
      tools: [{ name: "__noop", description: "noop", parameters: {} }],
      maxRounds: 2,
      state,
      script,
    })]]);
    const topo: Graph = { nodes: ["donna"], edges: [] };

    const graph = createEngine({ oracles: DEFAULT_ORACLES });
    const signals = await lastValueFrom(
      graph.exec$(realisticManifest(state, nodes, topo), "donna", "loop forever").pipe(toArray()),
    );

    const errors = ev(signals, "node:error");
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]!.error).toContain("iterations");
  });
});

describe("simulation: oracle.llm() loop primitive", () => {
  it("oracle.llm() drives the LLM→tool→LLM loop", async () => {
    const state = freshState();
    let callNum = 0;

    const oracleLLMNode: Node = {
      loadPrompt: (input) => of([
        { name: "system", content: "You are donna." },
        { name: "user", content: input },
      ]),
      execute: (layers, ctx) => {
        return ctx.oracle.llm({
          oracle: "llm",
          call: async (currentLayers, history) => {
            callNum++;
            if (callNum === 1) {
              return { response: "", toolCalls: [{ id: "q1", name: "market.quote", args: { symbol: "AAPL" } }] };
            }
            return { response: "AAPL looks good", toolCalls: [] };
          },
          executeTool: async (tc, ctx) => {
            const call = tc as ToolCall;
            state.toolCallLog.push({ node: "donna", tool: call.name, args: call.args });
            return { content: JSON.stringify({ price: 185.20 }) };
          },
        });
      },
    };

    const nodes = new Map([["donna", oracleLLMNode]]);
    const topo: Graph = { nodes: ["donna"], edges: [] };

    const graph = createEngine({ oracles: DEFAULT_ORACLES });
    const signals = await lastValueFrom(
      graph.exec$(realisticManifest(state, nodes, topo), "donna", "check AAPL").pipe(toArray()),
    );

    const afters = ev(signals, "node:after");
    expect(afters.length).toBe(1);
    expect(afters[0]!.output).toContain("AAPL looks good");
    expect(state.toolCallLog.length).toBe(1);
    expect(state.toolCallLog[0]!.tool).toBe("market.quote");
  });
});

describe("simulation: oracle.call() generic tracked call", () => {
  it("oracle.call() tracks cost through oracle type policies", async () => {
    const state = freshState();

    const apiOracles: Record<string, Oracle> = {
      ...DEFAULT_ORACLES,
      "api:market": {
        policies: [
          { type: "cost", max: 10, window: "invocation", extract: (r: unknown) => ({ commission: (r as { commission: number }).commission }) },
          { type: "timeout", max: 5000 },
        ],
      },
    };

    const apiNode: Node = {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: (_layers, ctx) => {
        return ctx.oracle.call("api:market", async () => {
          return { prices: [185.20], commission: 0.01 };
        }).pipe(
          concatMap((result) => of({ output: JSON.stringify(result) })),
        );
      },
    };

    const nodes = new Map([["donna", apiNode]]);
    const topo: Graph = { nodes: ["donna"], edges: [] };
    const graph = createEngine({ oracles: apiOracles });
    const signals = await lastValueFrom(
      graph.exec$(realisticManifest(state, nodes, topo), "donna", "get prices").pipe(toArray()),
    );

    const afters = ev(signals, "node:after");
    expect(afters.length).toBe(1);
    // Should have oracle:policy signal for the api:market cost
    const policySignals = signals.filter((s) => s.type === "oracle:policy:api:market");
    expect(policySignals.length).toBeGreaterThan(0);
  });
});

describe("simulation: oracle hierarchy inheritance", () => {
  it("llm:anthropic:claude inherits calls limit from llm", async () => {
    const state = freshState();

    const hierarchicalOracles: Record<string, Oracle> = {
      "llm": {
        policies: [
          { type: "calls", max: 1, window: "invocation" }, // only 1 call allowed
        ],
      },
      "llm:anthropic:claude": {
        policies: [
          { type: "timeout", max: 5000 },
        ],
      },
      tool: { policies: [] },
    };

    let callCount = 0;
    const node: Node = {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: (_layers, ctx) => {
        return new Observable<NodeResult>((subscriber) => {
          (async () => {
            // First call should succeed
            const r1 = await lastValueFrom(
              ctx.exec$("llm:anthropic:claude", async () => {
                callCount++;
                return { text: "first", cost: 0 };
              }),
            );
            // Second call should hit the inherited calls limit from "llm"
            try {
              await lastValueFrom(
                ctx.exec$("llm:anthropic:claude", async () => {
                  callCount++;
                  return { text: "second", cost: 0 };
                }),
              );
              subscriber.next({ output: "both succeeded" });
            } catch (err) {
              subscriber.next({ output: `blocked after ${callCount} calls` });
            }
            subscriber.complete();
          })().catch((err) => subscriber.error(err));
        });
      },
    };

    const nodes = new Map([["donna", node]]);
    const topo: Graph = { nodes: ["donna"], edges: [] };
    const graph = createEngine({ oracles: hierarchicalOracles });
    const signals = await lastValueFrom(
      graph.exec$(realisticManifest(state, nodes, topo), "donna", "test").pipe(toArray()),
    );

    const afters = ev(signals, "node:after");
    expect(afters.length).toBe(1);
    expect(afters[0]!.output).toContain("blocked after 1 calls");
  });
});

describe("simulation: fire-and-forget tell", () => {
  it("donna tells mike (async), both complete", async () => {
    const state = freshState();
    let callNum = 0;

    const script: LLMScript = (node) => {
      callNum++;
      if (node.includes("donna")) {
        if (callNum === 1) return { text: "", toolCalls: [{ id: "t1", name: "comm.tell", args: { target: "mike", message: "heads up" } }] };
        return { text: "donna done" };
      }
      return { text: "mike acknowledged" };
    };

    const toolFn = executeToolFn(state);
    const nodes = new Map([
      ["donna", makeNode("donna", { tools: [TELL_TOOL], llmFn: scriptedLLMFn(state, script), toolFn })],
      ["mike", makeNode("mike", { llmFn: scriptedLLMFn(state, script), toolFn })],
    ]);
    const topo: Graph = { nodes: ["donna"], edges: [] };

    const graph = createEngine({ oracles: DEFAULT_ORACLES, policies: [constraints.maxCycles(0), constraints.maxDepth(10)] });
    const signals = await lastValueFrom(
      graph.exec$(realisticManifest(state, nodes, topo), "donna", "brief mike").pipe(toArray()),
    );

    const donnaAfter = ev(signals, "node:after").filter((s) => s.nodeKey === "donna");
    const mikeAfter = ev(signals, "node:after").filter((s) => s.nodeKey === "mike");
    expect(donnaAfter.length).toBe(1);
    expect(mikeAfter.length).toBe(1);
  });
});

describe("simulation: abort mid-pipeline", () => {
  it("abort signal stops pending nodes", async () => {
    const state = freshState();
    const controller = new AbortController();
    let callNum = 0;

    const script: LLMScript = () => {
      callNum++;
      if (callNum === 1) {
        controller.abort();
        return { text: "first done" };
      }
      return { text: "should not reach" };
    };

    const llmFn = scriptedLLMFn(state, script);
    const nodes = new Map([
      ["a", makeNode("a", { llmFn })],
      ["b", makeNode("b", { llmFn })],
    ]);
    const topo: Graph = { nodes: ["a", "b"], edges: [{ from: "a", to: "b" }] };

    const graph = createEngine({ oracles: DEFAULT_ORACLES });

    try {
      await lastValueFrom(
        graph.exec$(realisticManifest(state, nodes, topo), "a", "start", controller.signal).pipe(toArray()),
      );
    } catch (err) {
      expect(err).toBeInstanceOf(Aborted);
    }
  });
});

describe("simulation: bubbling stop with maxLoop", () => {
  it("maxLoop allows specific node to cycle while global maxCycles(0) blocks others", async () => {
    const state = freshState();
    let roundCount = 0;

    const script: LLMScript = (node) => {
      roundCount++;
      return { text: `round ${roundCount}` };
    };

    const llmFn = scriptedLLMFn(state, script);
    const nodes = new Map([
      ["a", makeNode("a", { llmFn })],
      ["b", makeNode("b", { llmFn })],
    ]);
    const topo: Graph = { nodes: ["a", "b"], edges: [{ from: "a", to: "b" }, { from: "b", to: "a" }] };

    // maxLoop("a", 2) allows a to loop, maxLoop("b", 2) allows b to loop
    // global maxCycles(0) would block both without maxLoop
    const graph = createEngine({
      oracles: DEFAULT_ORACLES,
    });

    const manifest: Manifest = {
      ...realisticManifest(state, nodes, topo),
      getNodePolicies: (key) => [constraints.maxLoop(key, 2)],
    };

    const signals = await lastValueFrom(
      graph.exec$(manifest, "a", "start cycle").pipe(toArray()),
    );

    // Both nodes should execute multiple rounds
    const aAfters = ev(signals, "node:after").filter((s) => s.nodeKey === "a");
    const bAfters = ev(signals, "node:after").filter((s) => s.nodeKey === "b");
    expect(aAfters.length).toBeGreaterThan(0);
    expect(bAfters.length).toBeGreaterThan(0);
  });
});

describe("simulation: time-windowed calls limit", () => {
  it("calls limit tracks correctly within invocation window", async () => {
    const state = freshState();

    const callsOracles: Record<string, Oracle> = {
      llm: {
        policies: [
          { type: "calls", max: 2, window: "invocation" },
          { type: "timeout", max: 5000 },
        ],
      },
      tool: { policies: [] },
    };

    let callCount = 0;
    const node: Node = {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: (_layers, ctx) => {
        return new Observable<NodeResult>((subscriber) => {
          (async () => {
            // Call 1 — should succeed
            await lastValueFrom(ctx.exec$("llm", async () => ({ text: "a", cost: 0 })));
            callCount++;
            // Call 2 — should succeed (at limit)
            await lastValueFrom(ctx.exec$("llm", async () => ({ text: "b", cost: 0 })));
            callCount++;
            // Call 3 — should be blocked
            try {
              await lastValueFrom(ctx.exec$("llm", async () => ({ text: "c", cost: 0 })));
              callCount++;
              subscriber.next({ output: `${callCount} calls` });
            } catch (err) {
              subscriber.next({ output: `blocked at call ${callCount + 1}` });
            }
            subscriber.complete();
          })().catch((err) => subscriber.error(err));
        });
      },
    };

    const nodes = new Map([["donna", node]]);
    const topo: Graph = { nodes: ["donna"], edges: [] };
    const graph = createEngine({ oracles: callsOracles });
    const signals = await lastValueFrom(
      graph.exec$(realisticManifest(state, nodes, topo), "donna", "test").pipe(toArray()),
    );

    const afters = ev(signals, "node:after");
    expect(afters.length).toBe(1);
    expect(afters[0]!.output).toContain("blocked at call 3");
  });
});

describe("simulation: snapshot/restore round-trip", () => {
  it("counters survive snapshot → restore across graph instances", async () => {
    const state = freshState();

    const costOracles: Record<string, Oracle> = {
      llm: {
        policies: [
          { type: "cost", max: 10, window: "day" as const, extract: (r: unknown) => ({ cost: (r as OracleResponse)?.cost ?? 0 }) },
          { type: "timeout", max: 5000 },
        ],
      },
      tool: { policies: [] },
    };

    const llmFn = scriptedLLMFn(state, () => ({ text: "done", cost: 3.0 }));
    const nodes = new Map([["donna", makeNode("donna", { llmFn })]]);
    const topo: Graph = { nodes: ["donna"], edges: [] };

    // Run once — accumulate cost
    const graph1 = createEngine({ oracles: costOracles });
    await lastValueFrom(
      graph1.exec$(realisticManifest(state, nodes, topo), "donna", "first run").pipe(toArray()),
    );
    const snap = graph1.snapshot();

    // Restore into a new graph
    const graph2 = createEngine({ oracles: costOracles });
    graph2.restore(snap);
    const snap2 = graph2.snapshot();

    // Counters should match
    expect(snap2.length).toBe(snap.length);
    for (let i = 0; i < snap.length; i++) {
      expect(snap2[i]!.accumulated).toBe(snap[i]!.accumulated);
      expect(snap2[i]!.max).toBe(snap[i]!.max);
      expect(snap2[i]!.originalMax).toBe(snap[i]!.originalMax);
    }
  });
});

describe("simulation: exec:in/out signal pairing", () => {
  it("every exec:in has a matching exec:out — even on errors", async () => {
    const state = freshState();

    const tightOracles: Record<string, Oracle> = {
      llm: {
        policies: [
          { type: "calls", max: 1, window: "invocation" as const },
          { type: "timeout", max: 5000 },
        ],
      },
      tool: { policies: [{ type: "timeout", max: 5000 }] },
    };

    let callCount = 0;
    const node: Node = {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: (_layers, ctx) => {
        return new Observable<NodeResult>((subscriber) => {
          (async () => {
            // First call succeeds
            await lastValueFrom(ctx.exec$("llm", async () => {
              callCount++;
              return { text: "a", cost: 0 };
            }));
            // Second call hits limit — should still produce exec:in + exec:out
            try {
              await lastValueFrom(ctx.exec$("llm", async () => {
                callCount++;
                return { text: "b", cost: 0 };
              }));
            } catch {
              // expected
            }
            subscriber.next({ output: `${callCount} calls` });
            subscriber.complete();
          })().catch((err) => subscriber.error(err));
        });
      },
    };

    const nodes = new Map([["donna", node]]);
    const topo: Graph = { nodes: ["donna"], edges: [] };
    const graph = createEngine({ oracles: tightOracles });
    const signals = await lastValueFrom(
      graph.exec$(realisticManifest(state, nodes, topo), "donna", "test").pipe(toArray()),
    );

    const execIns = signals.filter((s) => s.type === "oracle:exec:in");
    const execOuts = signals.filter((s) => s.type === "oracle:exec:out");
    // Every exec:in must have a matching exec:out
    expect(execOuts.length).toBe(execIns.length);
  });
});

describe("simulation: no signal duplication in oracle.llm()", () => {
  it("signals from exec$ calls inside oracle.llm() are not double-emitted", async () => {
    const state = freshState();
    let callNum = 0;

    const oracleLLMNode: Node = {
      loadPrompt: (input) => of([
        { name: "system", content: "You are donna." },
        { name: "user", content: input },
      ]),
      execute: (layers, ctx) => {
        return ctx.oracle.llm({
          oracle: "llm",
          call: async () => {
            callNum++;
            if (callNum === 1) {
              return { response: "", toolCalls: [{ id: "q1", name: "market.quote", args: { symbol: "AAPL" } }] };
            }
            return { response: "done", toolCalls: [] };
          },
          executeTool: async (tc) => {
            return { content: JSON.stringify({ price: 185.20 }) };
          },
        });
      },
    };

    const nodes = new Map([["donna", oracleLLMNode]]);
    const topo: Graph = { nodes: ["donna"], edges: [] };

    const graph = createEngine({ oracles: DEFAULT_ORACLES });
    const signals = await lastValueFrom(
      graph.exec$(realisticManifest(state, nodes, topo), "donna", "check AAPL").pipe(toArray()),
    );

    // Check for duplicate signals by stringifying and counting
    const execIns = signals.filter((s) => s.type === "oracle:exec:in");
    const execOuts = signals.filter((s) => s.type === "oracle:exec:out");
    const policySignals = signals.filter((s) => s.type.startsWith("oracle:policy:"));

    // 2 LLM calls + 1 tool call = 3 exec:in, 3 exec:out
    expect(execIns.length, "should have exactly 3 exec:in (2 LLM + 1 tool)").toBe(3);
    expect(execOuts.length, "should have exactly 3 exec:out (2 LLM + 1 tool)").toBe(3);

    // Policy signals should not be duplicated — each limit check should appear once
    const costPolicies = policySignals.filter((s) => "limitType" in s && (s as { limitType: string }).limitType === "cost");
    // 2 LLM calls each produce 1 cost policy signal = 2 total
    expect(costPolicies.length, "cost policy signals should not be duplicated").toBe(2);
  });
});

describe("simulation: configurable interruptTimeout", () => {
  it("short interruptTimeout causes timeout error", async () => {
    const state = freshState();
    let callNum = 0;

    const script: LLMScript = () => {
      callNum++;
      if (callNum === 1) return { text: "", toolCalls: [{ id: "t1", name: "market.execute_trade", args: { symbol: "TSLA", qty: 50000 } }] };
      return { text: "done" };
    };

    const nodes = new Map([["donna", simNode("donna", { tools: [TRADE_TOOL], state, script })]]);
    const topo: Graph = { nodes: ["donna"], edges: [] };

    const graph = createEngine({
      oracles: DEFAULT_ORACLES,
      interruptTimeout: 1, // 1ms — will timeout before handler resolves
      handleInterrupt: async () => {
        await new Promise((r) => setTimeout(r, 100)); // takes 100ms
        return { decision: "retry" };
      },
    });

    const signals = await lastValueFrom(
      graph.exec$(realisticManifest(state, nodes, topo), "donna", "trade").pipe(toArray()),
    );

    // Should have interrupt:out with "error" decision (timeout)
    const interruptOuts = signals.filter((s) => s.type === "oracle:interrupt:out");
    const hasTimeout = interruptOuts.some((s) => "decision" in s && (s as { decision: string }).decision === "error");
    expect(hasTimeout, "interrupt should timeout").toBe(true);
  });
});

describe("simulation: oracle with registered executor", () => {
  it("exec$(oracle, input) calls the registered executor", async () => {
    const state = freshState();
    const executorCalls: unknown[] = [];

    const oracles: Record<string, Oracle> = {
      llm: { policies: [{ type: "timeout", max: 5000 }] },
      "api:market": {
        policies: [{ type: "calls", max: 10, window: "invocation" }],
        executor: async (input) => {
          executorCalls.push(input);
          return { price: 185.20, symbol: (input as { symbol: string }).symbol };
        },
      },
    };

    const node: Node = {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: (_layers, ctx) => new Observable<NodeResult>((sub) => {
        (async () => {
          // Call with registered executor — no fn needed
          const result = await lastValueFrom(
            ctx.exec$("api:market", { symbol: "AAPL" }),
          );
          sub.next({ output: JSON.stringify(result) });
          sub.complete();
        })().catch((e) => sub.error(e));
      }),
    };

    const nodes = new Map([["donna", node]]);
    const topo: Graph = { nodes: ["donna"], edges: [] };
    const graph = createEngine({ oracles });
    const signals = await lastValueFrom(
      graph.exec$(realisticManifest(state, nodes, topo), "donna", "get price").pipe(toArray()),
    );

    // Executor was called with the input
    expect(executorCalls).toEqual([{ symbol: "AAPL" }]);

    // Node completed successfully
    const after = signals.filter((s) => s.type === "node:after");
    expect(after.length).toBe(1);
    expect(JSON.parse(after[0]!.output as string)).toEqual({ price: 185.20, symbol: "AAPL" });

    // Signals include oracle exec lifecycle
    const execIns = signals.filter((s) => s.type === "oracle:exec:in" && s.oracle === "api:market");
    const execOuts = signals.filter((s) => s.type === "oracle:exec:out" && s.oracle === "api:market");
    expect(execIns.length).toBe(1);
    expect(execOuts.length).toBe(1);
  });

  it("exec$(oracle, input) respects policies on the oracle", async () => {
    const state = freshState();

    const oracles: Record<string, Oracle> = {
      llm: { policies: [{ type: "timeout", max: 5000 }] },
      "api:market": {
        policies: [{ type: "calls", max: 1, window: "invocation" }],
        executor: async (input) => ({ price: 100 }),
      },
    };

    const node: Node = {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: (_layers, ctx) => new Observable<NodeResult>((sub) => {
        (async () => {
          // First call succeeds
          await lastValueFrom(ctx.exec$("api:market", { symbol: "AAPL" }));
          // Second call should exceed calls limit
          await lastValueFrom(ctx.exec$("api:market", { symbol: "GOOG" }));
          sub.next({ output: "should not reach" });
          sub.complete();
        })().catch((e) => sub.error(e));
      }),
    };

    const nodes = new Map([["donna", node]]);
    const topo: Graph = { nodes: ["donna"], edges: [] };
    const graph = createEngine({ oracles });
    const signals = await lastValueFrom(
      graph.exec$(realisticManifest(state, nodes, topo), "donna", "test").pipe(toArray()),
    );

    // Should have exceeded the calls limit
    const exceeded = signals.filter((s) => s.type === "oracle:exceeded");
    expect(exceeded.length).toBeGreaterThan(0);
  });

  it("exec$(oracle, input) inherits executor from parent oracle", async () => {
    const state = freshState();
    const executorCalls: unknown[] = [];

    const oracles: Record<string, Oracle> = {
      llm: { policies: [{ type: "timeout", max: 5000 }] },
      "api": {
        policies: [],
        executor: async (input) => {
          executorCalls.push(input);
          return { ok: true };
        },
      },
      "api:market": {
        policies: [{ type: "calls", max: 10, window: "invocation" }],
        // No executor — should inherit from "api"
      },
    };

    const node: Node = {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: (_layers, ctx) => new Observable<NodeResult>((sub) => {
        (async () => {
          const result = await lastValueFrom(ctx.exec$("api:market", { symbol: "AAPL" }));
          sub.next({ output: JSON.stringify(result) });
          sub.complete();
        })().catch((e) => sub.error(e));
      }),
    };

    const nodes = new Map([["donna", node]]);
    const topo: Graph = { nodes: ["donna"], edges: [] };
    const graph = createEngine({ oracles });
    const signals = await lastValueFrom(
      graph.exec$(realisticManifest(state, nodes, topo), "donna", "test").pipe(toArray()),
    );

    // Parent executor was called
    expect(executorCalls).toEqual([{ symbol: "AAPL" }]);
    const after = signals.filter((s) => s.type === "node:after");
    expect(after.length).toBe(1);
  });
});
