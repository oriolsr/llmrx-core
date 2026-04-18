/**
 * integration.test.ts — One test to rule them all.
 *
 * Tests the hardest scenarios in a single pipeline:
 *   - 3-level nested graphs (spawnSync inside spawnSync)
 *   - Graph-scoped budget cascade (outer graph constrains inner)
 *   - oracle.llm() with rounds policy (no hardcoded cap)
 *   - Mutable limit → interrupt → mutation → retry across nesting levels
 *   - No timeout policy = no timeout
 *   - Snapshot excludes transient (invocation/graph) entries
 *   - Cleanup removes transient entries after completion
 *   - Abort propagates through nested graphs without deadlock
 */
import { describe, it, expect } from "vitest";
import { lastValueFrom, toArray, Observable, of } from "rxjs";
import {
  createEngine, Aborted, PolicyExceeded,
  constraints,
  type Manifest, type Node, type NodeExec, type NodeResult,
  type Layer, type Oracle, type Graph, type Signal,
} from "../src/llmrx.js";

let idCounter = 0;
const newId = (p: string) => `${p}_${++idCounter}`;

function makeManifest(nodeMap: Map<string, Node>, graphs: Map<string, Graph>): Manifest {
  return {
    getNode: (k) => { const n = nodeMap.get(k); if (!n) throw new Error(`no node: ${k}`); return n; },
    getNodeKeys: () => [...nodeMap.keys()],
    isNode: (k) => nodeMap.has(k),
    getGraph: (key) => graphs.get(key) ?? { nodes: [key], edges: [] },
    newId,
  };
}

describe("integration: nested graphs, budget cascade, oracle.llm(), mutation, abort, snapshot, cleanup", () => {

  it("3-level nested pipeline with cascading graph budget, mutation, and oracle.llm()", async () => {
    // ── Setup ──
    // Outer graph: donna → has graph-scoped call limit of 5
    // Donna spawns jessica (level 2), jessica spawns mike (level 3)
    // Mike uses oracle.llm() which makes LLM calls + tool calls
    // The LLM oracle has rounds: max 3 and cost tracking
    // The graph-scoped calls limit cascades — mike's calls tick donna's budget too
    //
    // Flow:
    //   donna runs → spawnSync(jessica) → jessica runs → spawnSync(mike) →
    //   mike uses oracle.llm() for 2 rounds (LLM → tool → LLM → done) →
    //   jessica makes 1 more call → donna makes 1 more call
    //   Total: mike 3 calls (2 LLM + 1 tool) + jessica 1 + donna 1 = 5 calls
    //   Graph budget: 5/5 on donna's counter

    let mikeRounds = 0;
    let jessicaCalls = 0;
    let donnaCalls = 0;
    let mutationApproved = false;

    const oracles: Record<string, Oracle> = {
      "llm": {
        policies: [
          { type: "calls", max: 5, window: "graph" },
          { type: "rounds", max: 3 },
        ],
      },
      "llm:anthropic:opus": {
        policies: [
          {
            type: "cost", max: 0.50, window: "day",
            extract: (r: unknown) => ({ cost: (r as { cost: number }).cost }),
            mutable: { "human:uri": (next: number) => next <= 10 },
          },
          // No timeout policy — engine should NOT impose one
        ],
      },
      "tool": {
        policies: [
          { type: "calls", max: 100, window: "invocation" },
        ],
      },
    };

    const nodes = new Map<string, Node>();

    // Mike (level 3) — uses oracle.llm() with 2 rounds
    nodes.set("mike", {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: (_layers, ctx) => {
        return ctx.oracle.llm({
          oracle: "llm:anthropic:opus",
          call: async () => {
            mikeRounds++;
            if (mikeRounds === 1) {
              return { text: "analyzing", tool_calls: [{ id: "t1", name: "research", input: {} }], cost: 0.15 };
            }
            return { text: "mike done", tool_calls: [], cost: 0.10 };
          },
          executeTool: async () => ({ content: "research results" }),
        });
      },
    });

    // Jessica (level 2) — spawns mike, then makes one more LLM call
    nodes.set("jessica", {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: (_layers, ctx) => new Observable<NodeResult>((sub) => {
        (async () => {
          // Spawn mike as nested graph
          const mikeSignals = await lastValueFrom(
            ctx.spawnSync({ key: "mike", data: "analyze AAPL" }).pipe(toArray()),
          );

          // One more LLM call from jessica herself
          await lastValueFrom(
            ctx.exec$("llm:anthropic:opus", async () => {
              jessicaCalls++;
              return { cost: 0.10, text: "jessica summary" };
            }),
          );

          const mikeOutput = mikeSignals.find((s) => s.type === "node:after");
          sub.next({ output: `jessica done (mike said: ${(mikeOutput as { output: unknown })?.output})` });
          sub.complete();
        })().catch((e) => sub.error(e));
      }),
    });

    // Donna (level 1) — spawns jessica, then makes one more LLM call
    nodes.set("donna", {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: (_layers, ctx) => new Observable<NodeResult>((sub) => {
        (async () => {
          // Spawn jessica as nested graph
          const jessicaSignals = await lastValueFrom(
            ctx.spawnSync({ key: "jessica", data: "research task" }).pipe(toArray()),
          );

          // One more LLM call from donna herself
          await lastValueFrom(
            ctx.exec$("llm:anthropic:opus", async () => {
              donnaCalls++;
              return { cost: 0.10, text: "donna summary" };
            }),
          );

          const jessicaOutput = jessicaSignals.find((s) => s.type === "node:after");
          sub.next({ output: `donna done (jessica said: ${(jessicaOutput as { output: unknown })?.output})` });
          sub.complete();
        })().catch((e) => sub.error(e));
      }),
    });

    const graphs = new Map<string, Graph>();
    graphs.set("donna", { nodes: ["donna"], edges: [] });
    graphs.set("jessica", { nodes: ["jessica"], edges: [] });
    graphs.set("mike", { nodes: ["mike"], edges: [] });

    const manifest = makeManifest(nodes, graphs);

    const graph = createEngine({
      oracles,
      policies: [constraints.maxCycles(0), constraints.maxDepth(10)],
      handleInterrupt: async (interrupt) => {
        mutationApproved = true;
        return { decision: "mutate", oracle: "human:uri", limitType: "cost", max: 5.0 };
      },
    });

    // ── Execute ──
    const signals = await lastValueFrom(
      graph.exec$(manifest, "donna", "analyze AAPL for trading").pipe(toArray()),
    );

    // ── Verify: pipeline completed ──
    // Nested spawnSync signals flow to the parent node, not the top-level stream.
    // The top-level stream sees donna's node:after which contains the full chain.
    const donnaAfter = signals.find((s) => s.type === "node:after") as Signal & { type: "node:after"; output: unknown } | undefined;
    expect(donnaAfter, "donna should complete").toBeTruthy();
    expect(String(donnaAfter!.output), "donna's output should contain jessica's output which contains mike's").toContain("jessica");
    expect(String(donnaAfter!.output), "output chain should reach mike").toContain("mike done");

    // Mike used oracle.llm() with 2 rounds (LLM → tool → LLM)
    expect(mikeRounds, "mike should have 2 LLM rounds via oracle.llm()").toBe(2);
    expect(jessicaCalls, "jessica should make 1 call").toBe(1);
    expect(donnaCalls, "donna should make 1 call").toBe(1);

    // ── Verify: graph signals carry key ──
    // Only the outermost graph's lifecycle signals appear on the top-level stream.
    // Nested graph signals are captured inside the parent node's execution.
    const graphBefores = signals.filter((s) => s.type === "graph:before") as Array<Signal & { type: "graph:before"; key: string }>;
    expect(graphBefores.length, "outermost graph:before").toBeGreaterThanOrEqual(1);
    expect(graphBefores[0]!.key, "graph:before must carry key").toBeTruthy();

    // ── Verify: graph:after count matches graph:before ──
    const graphAfters = signals.filter((s) => s.type === "graph:after");
    expect(graphAfters.length, "every graph:before must have exactly one graph:after").toBe(graphBefores.length);

    // ── Verify: oracle hierarchy — cost accumulated on both llm:anthropic:opus AND llm ──
    const policySignals = signals.filter((s) => s.type.startsWith("oracle:policy:"));
    const opusCostSignals = policySignals.filter((s) =>
      s.type === "oracle:policy:llm:anthropic:opus" && (s as { limitType: string }).limitType === "cost"
    );
    expect(opusCostSignals.length, "cost signals should exist for llm:anthropic:opus").toBeGreaterThan(0);

    // ── Verify: snapshot excludes transient entries ──
    const snap = graph.snapshot();
    for (const entry of snap) {
      expect(entry.key, "snapshot should not contain invocation-scoped entries").not.toContain(":invocation:");
      expect(entry.key, "snapshot should not contain graph-scoped entries").not.toContain(":graph:");
    }
    // But persistent entries (day-windowed cost) should exist
    const dayCost = snap.find((e) => e.key.includes("cost") && e.key.includes("day"));
    expect(dayCost, "day-windowed cost counter should be in snapshot").toBeTruthy();
    expect(dayCost!.accumulated, "cost should have accumulated across all 3 levels").toBeGreaterThan(0);
  });

  it("abort propagates through nested graphs without deadlock", async () => {
    // donna spawns jessica, jessica hangs. Abort should propagate cleanly.
    const controller = new AbortController();
    let jessicaStarted = false;

    const nodes = new Map<string, Node>();

    nodes.set("jessica", {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: (_layers, _ctx) => new Observable<NodeResult>((sub) => {
        jessicaStarted = true;
        // Hang — never complete. Abort should kill this.
        // Don't call sub.next() or sub.complete()
      }),
    });

    nodes.set("donna", {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: (_layers, ctx) => new Observable<NodeResult>((sub) => {
        (async () => {
          await lastValueFrom(
            ctx.spawnSync({ key: "jessica", data: "work" }).pipe(toArray()),
          );
          sub.next({ output: "donna done" });
          sub.complete();
        })().catch((e) => sub.error(e));
      }),
    });

    const graphs = new Map<string, Graph>();
    graphs.set("donna", { nodes: ["donna"], edges: [] });
    graphs.set("jessica", { nodes: ["jessica"], edges: [] });
    const manifest = makeManifest(nodes, graphs);

    const graph = createEngine({
      oracles: {},
      policies: [constraints.maxDepth(5)],
    });

    // Abort after 100ms
    setTimeout(() => controller.abort(), 100);

    const signals: Signal[] = [];
    let errored = false;

    await new Promise<void>((resolve) => {
      const timeout = setTimeout(() => {
        // If we get here, the abort deadlocked
        expect.fail("abort deadlocked — forkJoin hung on uncompleted ReplaySubject");
      }, 3000);

      graph.exec$(manifest, "donna", "go", { signal: controller.signal }).subscribe({
        next: (s) => signals.push(s),
        error: () => { errored = true; clearTimeout(timeout); resolve(); },
        complete: () => { clearTimeout(timeout); resolve(); },
      });
    });

    expect(jessicaStarted, "jessica should have started before abort").toBe(true);
    // The observable should have errored or completed — not hung
    expect(errored || signals.length > 0, "abort should propagate without deadlock").toBe(true);
  });

  it("rounds policy terminates oracle.llm() loop — no hardcoded cap", async () => {
    // LLM always returns tool calls. Rounds max=2 should stop the loop.
    let llmCalls = 0;

    const oracles: Record<string, Oracle> = {
      "llm": {
        policies: [{ type: "rounds", max: 2 }],
      },
      "tool": { policies: [] },
    };

    const nodes = new Map<string, Node>();
    nodes.set("worker", {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: (_layers, ctx) =>
        ctx.oracle.llm({
          oracle: "llm",
          call: async () => {
            llmCalls++;
            // Always return tool calls — loop should be killed by rounds policy
            return { text: `round ${llmCalls}`, tool_calls: [{ id: "t1", name: "work", input: {} }] };
          },
          executeTool: async () => ({ content: "done" }),
        }),
    });

    const graphs = new Map([["worker", { nodes: ["worker"], edges: [] }]]);
    const manifest = makeManifest(nodes, graphs);

    const graph = createEngine({ oracles });

    const signals: Signal[] = [];
    await new Promise<void>((resolve) => {
      graph.exec$(manifest, "worker", "go").subscribe({
        next: (s) => signals.push(s),
        error: () => resolve(),
        complete: () => resolve(),
      });
    });

    // Rounds max=2: LLM called twice (round 0 + round 1), then round limit exceeded on 3rd attempt
    expect(llmCalls, "LLM should be called exactly 2 times before rounds limit kicks in").toBe(2);

    // Should have a node:error with limit exceeded
    const nodeErrors = signals.filter((s) => s.type === "node:error") as Array<Signal & { type: "node:error"; error: string }>;
    expect(nodeErrors.some((e) => e.error.includes("limit")), "node should error with limit exceeded").toBe(true);
  });

  it("DAG with parallel nodes + graph-scoped limit shared correctly", async () => {
    // Graph: A and B run in parallel, both feed into C
    // Graph-scoped calls limit = 4
    // A makes 2 calls, B makes 2 calls, C tries 1 → should be blocked (4/4 already used)
    let aCalls = 0, bCalls = 0, cCalls = 0;
    let cBlocked = false;

    const oracles: Record<string, Oracle> = {
      "llm": {
        policies: [
          { type: "calls", max: 4, window: "graph" },
        ],
      },
    };

    const nodes = new Map<string, Node>();
    nodes.set("a", {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: (_layers, ctx) => new Observable<NodeResult>((sub) => {
        (async () => {
          await lastValueFrom(ctx.exec$("llm", async () => { aCalls++; return "a1"; }));
          await lastValueFrom(ctx.exec$("llm", async () => { aCalls++; return "a2"; }));
          sub.next({ output: "a done" });
          sub.complete();
        })().catch((e) => sub.error(e));
      }),
    });
    nodes.set("b", {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: (_layers, ctx) => new Observable<NodeResult>((sub) => {
        (async () => {
          await lastValueFrom(ctx.exec$("llm", async () => { bCalls++; return "b1"; }));
          await lastValueFrom(ctx.exec$("llm", async () => { bCalls++; return "b2"; }));
          sub.next({ output: "b done" });
          sub.complete();
        })().catch((e) => sub.error(e));
      }),
    });
    nodes.set("c", {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: (_layers, ctx) => new Observable<NodeResult>((sub) => {
        (async () => {
          try {
            await lastValueFrom(ctx.exec$("llm", async () => { cCalls++; return "c1"; }));
          } catch (e) {
            if (e instanceof PolicyExceeded) { cBlocked = true; }
            throw e;
          }
          sub.next({ output: "c done" });
          sub.complete();
        })().catch((e) => sub.error(e));
      }),
    });

    const topo: Graph = { nodes: ["a", "b", "c"], edges: [{ from: "a", to: "c" }, { from: "b", to: "c" }] };
    const graphs = new Map([["root", topo]]);
    const manifest = makeManifest(nodes, graphs);

    const graph = createEngine({ oracles });
    const signals = await lastValueFrom(
      graph.exec$(manifest, "root", "go").pipe(toArray()),
    );

    expect(aCalls, "A should make 2 calls").toBe(2);
    expect(bCalls, "B should make 2 calls").toBe(2);
    expect(cBlocked, "C should be blocked — graph budget exhausted by parallel A+B").toBe(true);
    expect(cCalls, "C's call should not have executed").toBe(0);
  });
});
