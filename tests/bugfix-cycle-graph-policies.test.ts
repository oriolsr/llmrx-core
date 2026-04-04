/**
 * BUG: cycleRound$ policy resolution skips graph-level policies.
 * Only checks node policies + global policies, missing getGraphPolicies().
 * A maxCycles constraint registered at graph level won't prevent cycling.
 */
import { describe, it, expect } from "vitest";
import { lastValueFrom, toArray, Observable, of } from "rxjs";
import {
  createEngine, constraints,
  type Manifest, type Node, type NodeExec, type NodeResult,
  type Oracle, type Graph, type Signal,
} from "../src/llmrx.js";

let idCounter = 0;
const newId = (p: string) => `${p}_${++idCounter}`;

describe("BUG: cycleRound$ skips graph-level policies", () => {
  it("graph-level maxCycles(1) limits cycle iterations", async () => {
    const oracles: Record<string, Oracle> = {
      llm: { policies: [{ type: "timeout", max: 5000 }] },
    };

    let execCount = 0;

    const nodes = new Map<string, Node>();
    nodes.set("looper", {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: () => new Observable<NodeResult>((sub) => {
        execCount++;
        sub.next({ output: `run-${execCount}` });
        sub.complete();
      }),
    });

    // Self-referencing cycle: looper → looper
    const topo: Graph = {
      nodes: ["looper"],
      edges: [{ from: "looper", to: "looper" }],
    };

    const manifest: Manifest = {
      getNode: (k) => { const n = nodes.get(k); if (!n) throw new Error(`no: ${k}`); return n; },
      getNodeKeys: () => [...nodes.keys()],
      isNode: (k) => nodes.has(k),
      getGraph: () => topo,
      // maxCycles(1) at graph level — should allow only 1 cycle iteration
      getGraphPolicies: () => [constraints.maxCycles(1)],
      newId,
    };

    const graph = createEngine({ oracles });

    const signals = await lastValueFrom(
      graph.exec$(manifest, "looper", "go").pipe(toArray()),
    );

    // With maxCycles(1), the looper should execute at most 2 times
    // (initial + 1 cycle). If graph policies are skipped, it might run more.
    expect(execCount, "looper should be limited by graph-level maxCycles(1)").toBeLessThanOrEqual(2);
  }, 10000);
});
