/**
 * BUG: checkAncestry only checks node-level + global policies,
 * but skips graph-level policies from manifest.getGraphPolicies().
 * Graph-level constraints like maxCycles(0) are bypassed.
 */
import { describe, it, expect } from "vitest";
import { lastValueFrom, toArray, Observable, of } from "rxjs";
import {
  createEngine, constraints,
  type Manifest, type Node, type NodeExec, type NodeResult,
  type Layer, type Oracle, type Graph, type Signal,
} from "../src/llmrx.js";

let idCounter = 0;
const newId = (p: string) => `${p}_${++idCounter}`;

describe("BUG: checkAncestry skips graph-level policies", () => {
  it("graph-level maxCycles(0) should block self-spawn via checkAncestry", async () => {
    const oracles: Record<string, Oracle> = {
      llm: { policies: [{ type: "timeout", max: 5000 }] },
    };

    let ancestryResult: string | null = "not checked";

    const nodes = new Map<string, Node>();
    nodes.set("caller", {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: (_layers, ctx) => {
        return new Observable<NodeResult>((sub) => {
          // Node "caller" is already in its own ancestry.
          // maxCycles(0) at graph level should block re-spawning "caller".
          ancestryResult = ctx.checkAncestry("caller");
          sub.next({ output: "checked" });
          sub.complete();
        });
      },
    });

    const topo: Graph = { nodes: ["caller"], edges: [] };
    const manifest: Manifest = {
      getNode: (k) => { const n = nodes.get(k); if (!n) throw new Error(`no: ${k}`); return n; },
      getNodeKeys: () => [...nodes.keys()],
      isNode: (k) => nodes.has(k),
      getGraph: () => topo,
      // maxCycles(0) is a GRAPH-LEVEL policy, not global
      getGraphPolicies: () => [constraints.maxCycles(0)],
    };

    const graph = createEngine({ oracles });
    await lastValueFrom(graph.exec$(manifest, "caller", "go").pipe(toArray()));

    // If graph policies are checked, "caller" should be blocked (it's already in ancestry)
    expect(ancestryResult, "checkAncestry should block self-spawn via graph-level maxCycles(0)").not.toBeNull();
    expect(ancestryResult).toContain("caller");
  });
});
