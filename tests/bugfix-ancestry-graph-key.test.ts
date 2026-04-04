/**
 * BUG: checkAncestry calls getGraphPolicies() without the required key argument.
 * The Manifest interface defines: getGraphPolicies?(key: string): Policy[] | null
 * But checkAncestry calls: ctx.manifest.getGraphPolicies?.() — no key.
 *
 * This means getGraphPolicies receives undefined as key, so implementations
 * that use the key to return different policies per graph entry point will
 * return the wrong policies (or none).
 */
import { describe, it, expect } from "vitest";
import { lastValueFrom, toArray, Observable, of } from "rxjs";
import {
  createEngine, constraints,
  type Manifest, type Node, type NodeExec, type NodeResult,
  type Oracle, type Graph, type Policy,
} from "../src/llmrx.js";

describe("BUG: checkAncestry passes no key to getGraphPolicies()", () => {
  it("getGraphPolicies receives the correct root node key", async () => {
    const oracles: Record<string, Oracle> = {
      llm: { policies: [{ type: "timeout", max: 5000 }] },
    };

    let receivedKey: string | undefined;
    let ancestryResult: string | null = "not checked";

    const nodes = new Map<string, Node>();
    nodes.set("entry", {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: (_layers, ctx) => new Observable<NodeResult>((sub) => {
        ancestryResult = ctx.checkAncestry("entry");
        sub.next({ output: "done" });
        sub.complete();
      }),
    });

    const topo: Graph = { nodes: ["entry"], edges: [] };
    const manifest: Manifest = {
      getNode: (k) => { const n = nodes.get(k); if (!n) throw new Error(`no: ${k}`); return n; },
      getNodeKeys: () => [...nodes.keys()],
      isNode: (k) => nodes.has(k),
      getGraph: () => topo,
      getGraphPolicies: (key: string) => {
        receivedKey = key;
        return [constraints.maxCycles(0)];
      },
    };

    const graph = createEngine({ oracles });
    await lastValueFrom(graph.exec$(manifest, "entry", "go").pipe(toArray()));

    // getGraphPolicies should receive the root node key, not undefined
    expect(receivedKey, "getGraphPolicies should receive the node key").toBe("entry");
    // And the policy should work
    expect(ancestryResult, "checkAncestry should block via graph policy").not.toBeNull();
  });
});
