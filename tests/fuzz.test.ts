/**
 * llmrx fuzz test — N random configs. Structural invariants only.
 * Seed logged on failure. Reproduce with FUZZ_SEED=<seed> FUZZ_N=1
 *
 * In the oracle engine model:
 *   - Node.execute() owns the LLM→tool→LLM loop
 *   - LLM calls go through ctx.exec$("llm", ...)
 *   - Tool calls go through ctx.exec$("tool", ...)
 *   - All limits/constraints are policies on oracle types
 *   - No separate LimitStack — oracle types carry policies
 */
import { describe, it, expect } from "vitest";
import { lastValueFrom, toArray, of, Observable } from "rxjs";
import {
  createEngine, Interrupt, Denied, PolicyExceeded, Aborted,
  constraints,
  type Manifest, type CustomPolicy,
  type InterruptCtx, type InterruptResult, type Node, type Graph,
  type Layer, type Oracle, type NodeExec, type NodeResult,
  type Signal, type Policy,
} from "../src/llmrx.js";

// Consumer-owned types (not part of engine)
interface ToolCall { id: string; name: string; args: Record<string, unknown> }
interface OracleResponse { text: string; toolCalls: ToolCall[]; cost: number }

// ── PRNG ──
class Rng {
  private s: number;
  constructor(seed: number) { this.s = seed; }
  next() { this.s = (this.s * 1664525 + 1013904223) & 0xffffffff; return (this.s >>> 0) / 0x100000000; }
  int(a: number, b: number) { return Math.floor(this.next() * (b - a + 1)) + a; }
  bool(p = 0.5) { return this.next() < p; }
  pick<T>(a: readonly T[]) { return a[this.int(0, a.length - 1)]!; }
  pickN<T>(a: readonly T[], n: number) { const c = [...a]; return Array.from({ length: Math.min(n, c.length) }, () => c.splice(this.int(0, c.length - 1), 1)[0]!); }
  str(n = 6) { return Array.from({ length: n }, () => "abcdefghijklmnopqrstuvwxyz"[this.int(0, 25)]).join(""); }
}

interface ToolDef { name: string; description: string; parameters: Record<string, unknown> }
const NAMES = ["donna", "mike", "harvey", "rachel", "louis", "jessica", "warren", "benjamin"];

const ASK_TOOL: ToolDef = { name: "comm.ask", description: "Ask another node or human", parameters: { type: "object", properties: { target: {}, message: { type: "string" } }, required: ["target", "message"] } };
const TELL_TOOL: ToolDef = { name: "comm.tell", description: "Tell another node or human (fire and forget)", parameters: { type: "object", properties: { target: {}, message: { type: "string" } }, required: ["target", "message"] } };

// ── GENERATORS ──
function gen(seed: number) {
  const r = new Rng(seed);

  const nodes = r.pickN(NAMES, r.int(1, 5));
  const edges: Array<{ from: string; to: string }> = [];
  for (let i = 0; i < nodes.length; i++)
    for (let j = i + 1; j < nodes.length; j++)
      if (r.bool(0.4)) edges.push({ from: nodes[i]!, to: nodes[j]! });

  const cyclic = nodes.length >= 2 && r.bool(0.2);
  const cycleMaxRevisits = cyclic ? r.int(1, 3) : 0;
  if (cyclic) {
    const backEdgeCount = r.int(1, Math.min(2, nodes.length - 1));
    for (let b = 0; b < backEdgeCount; b++) {
      const from = nodes[r.int(1, nodes.length - 1)]!;
      const to = nodes[r.int(0, nodes.indexOf(from) - 1 < 0 ? 0 : nodes.indexOf(from) - 1)]!;
      if (from !== to) edges.push({ from, to });
    }
  }
  const useNodeTools = r.bool(0.4);
  const topo: Graph = { nodes, edges };

  const allNames = [...new Set([...nodes, ...r.pickN(NAMES, 3)])];

  const nodeToolDefs = new Map<string, ToolDef[]>();
  const nodeMaxRounds = new Map<string, number | undefined>();

  for (const key of allNames) {
    const domainDefs: ToolDef[] = Array.from({ length: r.int(1, 4) }, () => ({
      name: `t_${r.str(4)}`, description: "", parameters: {},
    }));
    const allDefs = useNodeTools ? [...domainDefs, ASK_TOOL, TELL_TOOL] : domainDefs;
    const maxToolRounds = r.bool(0.7) ? r.int(1, 5) : undefined;
    nodeToolDefs.set(key, allDefs);
    nodeMaxRounds.set(key, maxToolRounds);
  }

  const humans = [`human_${r.str(3)}`, `human_${r.str(3)}`];

  const policyMode = r.pick(["allow", "allow", "allow", "deny", "interrupt"] as const);
  const toolInterruptRate = r.bool(0.2) ? 0.25 : 0;

  return { topo, allNames, nodeToolDefs, nodeMaxRounds, humans, policyMode, toolInterruptRate, useNodeTools, cyclic, cycleMaxRevisits, seed };
}

function mocks(s: ReturnType<typeof gen>) {
  let ids = 0;
  const MAX_CALLS = 15;
  const lr = new Rng(s.seed + 1000), tr = new Rng(s.seed + 2000), pr = new Rng(s.seed + 3000);
  let modelCalls = 0;

  const llmFn = async (layers: Layer[]): Promise<OracleResponse> => {
    modelCalls++;
    if (modelCalls > MAX_CALLS) return { text: "cap", toolCalls: [], cost: 0 };
    const cost = lr.next() * 0.1;
    const text = `r_${modelCalls}`;
    const toolCalls: ToolCall[] = [];

    const toolsLayer = layers.find((l) => l.name === "tools");
    const tools = (toolsLayer?.content as ToolDef[] | undefined) ?? [];
    if (tools.length && modelCalls <= 4 && lr.bool(0.4)) {
      const domainTools = tools.filter((t) => !t.name.startsWith("comm."));
      if (lr.bool(0.3) && s.useNodeTools) {
        const allTargets = [...s.allNames, ...s.humans];
        toolCalls.push({ id: `c_${modelCalls}`, name: "comm.ask", args: { target: lr.pick(allTargets), message: `ask_${lr.str(6)}` } });
      }
      else if (lr.bool(0.2) && s.useNodeTools) {
        const allTargets = [...s.allNames, ...s.humans];
        toolCalls.push({ id: `c_${modelCalls}`, name: "comm.tell", args: { target: lr.pick(allTargets), message: `tell_${lr.str(6)}` } });
      }
      else if (domainTools.length) {
        toolCalls.push({ id: `c_${modelCalls}`, name: lr.pick(domainTools).name, args: { v: lr.str(4) } });
      }
    }

    return { text, toolCalls, cost };
  };

  const toolFn = async (call: ToolCall, nodeKey: string, ctx: NodeExec): Promise<{ result: unknown; content: string; signals?: Signal[] }> => {
    if (call.name === "comm.ask") {
      const target = call.args.target as string;
      const msg = call.args.message as string;
      const reason = ctx.checkAncestry(target);
      if (reason) {
        return { result: { blocked: true, reason }, content: `blocked: ${reason}` };
      }
      if (ctx.isNode(target)) {
        const signals = await lastValueFrom(ctx.spawnSync({ key: target, data: msg }).pipe(toArray()));
        const after = signals.find((sig) => sig.type === "node:after") as { output: string } | undefined;
        return { result: { response: after?.output ?? "no response" }, content: after?.output ?? "no response", signals };
      }
      return { result: { approved: true }, content: JSON.stringify({ approved: true }) };
    }
    if (call.name === "comm.tell") {
      const target = call.args.target as string;
      const msg = call.args.message as string;
      const reason = ctx.checkAncestry(target);
      if (!reason && ctx.isNode(target)) {
        ctx.spawnAsync({ key: target, data: msg });
      }
      return { result: { acknowledged: true }, content: "acknowledged" };
    }
    if (call.name === "__noop" || call.name === "__ask_human") {
      return { result: { ok: true }, content: "ok" };
    }
    if (tr.next() < s.toolInterruptRate) throw new Interrupt({ tool: call.name, node: nodeKey });
    return { result: { ok: true, data: tr.str(8) }, content: JSON.stringify({ ok: true }) };
  };

  const nodeSpecs = new Map<string, Node>();
  for (const key of s.allNames) {
    const allDefs = s.nodeToolDefs.get(key) ?? [];
    const maxRounds = s.nodeMaxRounds.get(key);
    const maxIterations = maxRounds ?? 5;
    const isTopologyNode = s.topo.nodes.includes(key);
    const effectiveMaxIterations = isTopologyNode ? maxIterations : 1;

    nodeSpecs.set(key, {
      loadPrompt: (input, state) => of([
        { name: "system", content: `You are ${key}. Counters: ${JSON.stringify(state.counters("llm"))}` },
        { name: "tools", content: allDefs },
        { name: "user", content: input },
      ]),
      execute(layers: Layer[], ctx: NodeExec): Observable<NodeResult> {
        return new Observable<NodeResult>((subscriber) => {
          (async () => {
            let currentLayers = [...layers];
            let round = 0;

            while (round <= effectiveMaxIterations) {
              let response: OracleResponse;
              try {
                response = await lastValueFrom(
                  ctx.exec$("llm", () => llmFn(currentLayers)),
                );
              } catch (err) {
                throw err;
              }

              if (!response.toolCalls || response.toolCalls.length === 0) {
                subscriber.next({ output: response.text });
                subscriber.complete();
                return;
              }

              round++;
              if (round > effectiveMaxIterations) {
                subscriber.next({ output: response.text });
                subscriber.complete();
                return;
              }

              for (const tc of response.toolCalls) {
                try {
                  const toolResult = await lastValueFrom(
                    ctx.exec$("tool", async () => {
                      return toolFn(tc, key, ctx);
                    }, tc.args),
                  );
                  currentLayers.push({ name: "tool", content: toolResult.content });
                } catch (err) {
                  if (err instanceof Denied) {
                    currentLayers.push({ name: "tool", content: `denied:${tc.id}:${err.reason}` });
                    continue;
                  }
                  throw err;
                }
              }
            }

            subscriber.next({ output: "max iterations" });
            subscriber.complete();
          })().catch((err) => subscriber.error(err));
        });
      },
    });
  }

  const ancestryConstraints: CustomPolicy[] = s.cyclic
    ? [constraints.maxCycles(s.cycleMaxRevisits - 1)]
    : [constraints.maxCycles(0)];

  const manifest: Manifest = {
    getNode: (k: string) => {
      const a = nodeSpecs.get(k); if (!a) throw new Error(`no node: ${k}`);
      return a;
    },
    getNodeKeys: () => [...nodeSpecs.keys()],
    isNode: (k: string) => nodeSpecs.has(k) && !k.startsWith("human_"),
    getGraph: (key: string) => s.topo.nodes.includes(key) ? s.topo : { nodes: [key], edges: [] },
    getNodePolicies: () => s.cyclic ? [constraints.maxCycles(s.cycleMaxRevisits - 1)] : null,
    newId: (p: string) => `${p}_${++ids}`,
  };

  const policy: CustomPolicy = {
    policy: (a) => {
      if (s.policyMode === "allow") return { approval: "allow" };
      if (s.policyMode === "deny" && pr.bool(0.15)) return { approval: "deny", reason: `deny_${a.oracle}` };
      if (s.policyMode === "interrupt" && pr.bool(0.1)) {
        return { approval: "interrupt", interrupt: new Interrupt({ target: pr.pick(s.humans), oracle: a.oracle, history: a.history }) };
      }
      return { approval: "allow" };
    },
  };

  const oracles: Record<string, Oracle> = {
    llm: {
      policies: [
        { type: "cost", max: 100, window: "invocation", extract: (r: unknown) => ({ cost: (r as OracleResponse)?.cost ?? 0 }) },
        { type: "timeout", max: 5000 },
        { type: "retries", max: 1 },
      ],
    },
    tool: {
      policies: [
        { type: "timeout", max: 5000 },
      ],
    },
  };

  const handleInterrupt = async (_interrupt: Interrupt, _ctx: InterruptCtx): Promise<InterruptResult> => {
    const ir = new Rng(s.seed + 5000 + ids);
    const r = ir.next();
    if (r < 0.5) return { decision: "deny", reason: "fuzz_deny" };
    return { decision: "result", value: { fuzz: true } };
  };

  return { manifest, policy, oracles, handleInterrupt, ancestryConstraints };
}

// ── INVARIANTS ──
function check(signals: Signal[], seed: number) {
  const t = `[seed=${seed}]`;
  expect(signals.length, `${t} no signals`).toBeGreaterThan(0);

  // 1. graph:before → graph:after (1:1)
  const gStarts = signals.filter((e) => e.type === "graph:before");
  const gDones = signals.filter((e) => e.type === "graph:after");
  for (const e of gStarts) if (e.type === "graph:before")
    expect(gDones.some((d) => d.type === "graph:after" && d.graphId === e.graphId), `${t} graph:before without after ${e.graphId}`).toBe(true);

  // 2. node:before → node:after|error
  for (const e of signals) if (e.type === "node:before")
    expect(signals.some((d) => (d.type === "node:after" || d.type === "node:error") && d.nodeKey === e.nodeKey), `${t} node orphan ${e.nodeKey}`).toBe(true);

  // 3. ordering: graph:before before its graph:after
  for (const e of signals) {
    if (e.type === "graph:before") {
      const si = signals.indexOf(e);
      const ei = signals.findIndex((d) => d.type === "graph:after" && d.graphId === e.graphId);
      if (ei >= 0) expect(si, `${t} graph:after before graph:before ${e.graphId}`).toBeLessThan(ei);
    }
  }

  // 5. oracle:interrupt:in has resolution
  for (const e of signals) {
    if (e.type === "oracle:interrupt:in") {
      const resolved = signals.some((d) =>
        (d.type === "oracle:interrupt:out" && d.nodeKey === e.nodeKey) ||
        (d.type === "node:error" && d.nodeKey === e.nodeKey),
      );
      expect(resolved, `${t} oracle:interrupt:out without resolution from ${e.nodeKey}`).toBe(true);
    }
  }

  // 6. exec$ activity evidence — check for node completions or policy signals
  const execEvidence = signals.filter((e) =>
    e.type.startsWith("oracle:policy:") ||
    e.type === "node:after" || e.type === "node:error",
  );
  expect(execEvidence.length, `${t} no exec$ evidence`).toBeGreaterThan(0);

  // 7. oracle:exec:in/out pairing — every exec:in has a matching exec:out
  const execIns = signals.filter((e) => e.type === "oracle:exec:in");
  const execOuts = signals.filter((e) => e.type === "oracle:exec:out");
  expect(execOuts.length, `${t} exec:in/out mismatch: ${execIns.length} in, ${execOuts.length} out`).toBe(execIns.length);

  // 8. every signal carries the full envelope (nodeKey, graphId, ancestry)
  for (const e of signals) {
    if (e.type.startsWith("x:")) continue;
    expect(typeof e.nodeKey, `${t} missing nodeKey on ${e.type}`).toBe("string");
    expect(e.nodeKey.length, `${t} empty nodeKey on ${e.type}`).toBeGreaterThan(0);
    expect(typeof e.graphId, `${t} missing graphId on ${e.type}`).toBe("string");
    expect(e.graphId.length, `${t} empty graphId on ${e.type}`).toBeGreaterThan(0);
    expect(Array.isArray(e.ancestry), `${t} missing ancestry on ${e.type}`).toBe(true);
    expect(e.ancestry.length, `${t} empty ancestry on ${e.type}`).toBeGreaterThan(0);
  }
}

// ── THE TEST ──
const N = Number(process.env.FUZZ_N ?? 100);
const SEED = Number(process.env.FUZZ_SEED ?? Date.now());

describe("llmrx fuzz", () => {
  it(`${N} random configs (seed: ${SEED})`, async () => {
    const stats = { runs: 0, signals: 0, errors: 0, denied: 0, interrupted: 0, edges: 0, cyclic: 0 };

    for (let i = 0; i < N; i++) {
      const s = gen(SEED + i);
      const m = mocks(s);
      const graph = createEngine({
        oracles: m.oracles,
        policies: [m.policy, ...m.ancestryConstraints],
        handleInterrupt: m.handleInterrupt,
      });

      stats.runs++;
      stats.edges += s.topo.edges.length;
      if (s.cyclic) stats.cyclic++;
      const signals: Signal[] = [];
      try {
        const all = await lastValueFrom(graph.exec$(m.manifest, s.topo.nodes[0]!, `msg_${s.seed}`).pipe(toArray()));
        signals.push(...all);
      } catch (err) {
        stats.errors++;
        const ok = err instanceof PolicyExceeded || err instanceof Denied || err instanceof Interrupt || err instanceof Aborted ||
          (err instanceof Error && /Timeout|no node|unknown target/.test(err.message));
        expect(ok, `[seed=${SEED + i}] unexpected: ${err instanceof Error ? err.constructor.name + ": " + err.message : err}`).toBe(true);
        continue;
      }

      stats.signals += signals.length;
      stats.denied += signals.filter((e) => e.type === "node:error" && "error" in e && String(e.error).includes("denied")).length;
      stats.interrupted += signals.filter((e) => e.type === "oracle:interrupt:in").length;

      if (signals.length) {
        if (N <= 5) console.log(`[seed=${SEED + i}] signals:`, signals.map((s) => s.type));
        check(signals, SEED + i);
      }
    }

    if (N >= 50) {
      expect(stats.signals, "no signals across all runs").toBeGreaterThan(0);
      console.log(`\n📊 Fuzz stats (${N} runs, seed ${SEED}):`);
      console.log(`   signals: ${stats.signals} | errors: ${stats.errors} | denied: ${stats.denied}`);
      console.log(`   interrupts: ${stats.interrupted} | edges: ${stats.edges} | cyclic: ${stats.cyclic}`);
    }
  }, 60_000);
});
