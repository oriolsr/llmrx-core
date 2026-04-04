/**
 * BUG: resolveNewId() calls makeDefaultNewId() every time manifest.newId is
 * undefined. Each call creates a fresh closure with counter=0, so IDs from
 * different call sites (execAction$, resolveInterrupt$, executeNode$, run$)
 * will collide — e.g., all generate "or_1", "po_1" independently.
 *
 * IDs should be unique across an entire graph execution.
 */
import { describe, it, expect } from "vitest";
import { lastValueFrom, toArray, Observable, of } from "rxjs";
import {
  createEngine,
  type Manifest, type Node, type NodeExec, type NodeResult,
  type Layer, type Oracle, type Graph, type Signal,
} from "../src/llmrx.js";

describe("BUG: resolveNewId creates fresh counter per call site", () => {
  it("all IDs in a graph execution are unique when manifest.newId is not provided", async () => {
    const oracles: Record<string, Oracle> = {
      llm: {
        policies: [
          { type: "calls", max: 10, window: "invocation" },
          { type: "timeout", max: 5000 },
        ],
      },
    };

    const nodes = new Map<string, Node>();
    nodes.set("a", {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: (_layers: Layer[], ctx: NodeExec) => new Observable<NodeResult>((sub) => {
        (async () => {
          await lastValueFrom(ctx.exec$("llm", async () => "r1"));
          await lastValueFrom(ctx.exec$("llm", async () => "r2"));
          sub.next({ output: "done" });
          sub.complete();
        })().catch((e) => sub.error(e));
      }),
    });
    nodes.set("b", {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: (_layers: Layer[], ctx: NodeExec) => new Observable<NodeResult>((sub) => {
        (async () => {
          await lastValueFrom(ctx.exec$("llm", async () => "r1"));
          sub.next({ output: "done" });
          sub.complete();
        })().catch((e) => sub.error(e));
      }),
    });

    const topo: Graph = { nodes: ["a", "b"], edges: [{ from: "a", to: "b" }] };
    // Deliberately NOT providing newId — uses default
    const manifest: Manifest = {
      getNode: (k) => { const n = nodes.get(k); if (!n) throw new Error(`no: ${k}`); return n; },
      getNodeKeys: () => [...nodes.keys()],
      isNode: (k) => nodes.has(k),
      getGraph: () => topo,
      // no newId provided
    };

    const graph = createEngine({ oracles });
    const signals = await lastValueFrom(
      graph.exec$(manifest, "a", "go").pipe(toArray()),
    );

    // Collect all IDs from signals
    const allIds = new Set<string>();
    const duplicates: string[] = [];
    for (const s of signals) {
      for (const key of ["nodeId", "oracleId", "policyId", "graphId"] as const) {
        const id = (s as Record<string, string>)[key];
        if (id && id !== "") {
          if (allIds.has(id)) {
            duplicates.push(id);
          }
          allIds.add(id);
        }
      }
    }

    // nodeId for different nodes should be different
    const nodeIds = new Set(
      signals
        .filter((s) => s.type === "node:before")
        .map((s) => (s as { nodeId: string }).nodeId),
    );
    expect(nodeIds.size, "each node:before should have a unique nodeId").toBe(2);

    // oracleId for different exec$ calls should be different
    const oracleIds = signals
      .filter((s) => s.type === "oracle:exec:in")
      .map((s) => (s as { oracleId: string }).oracleId);
    const uniqueOracleIds = new Set(oracleIds);
    expect(uniqueOracleIds.size, `all oracle:exec:in should have unique oracleIds, got ${JSON.stringify(oracleIds)}`).toBe(oracleIds.length);
  });
});
