/**
 * BUG: Off-by-one in oracle.llm() round loop.
 *
 * With rounds policy max=2, the loop should allow exactly 2 tool-call rounds
 * before the rounds limit is exceeded.
 */
import { describe, it, expect } from "vitest";
import { lastValueFrom, toArray, Observable, of } from "rxjs";
import {
  createEngine, PolicyExceeded,
  type Manifest, type Node, type NodeExec, type NodeResult,
  type Layer, type Oracle, type Graph, type Signal,
} from "../src/llmrx.js";

let idCounter = 0;
const newId = (p: string) => `${p}_${++idCounter}`;

function manifest(nodeMap: Map<string, Node>, topo: Graph): Manifest {
  return {
    getNode: (k) => { const n = nodeMap.get(k); if (!n) throw new Error(`no: ${k}`); return n; },
    getNodeKeys: () => [...nodeMap.keys()],
    isNode: (k) => nodeMap.has(k),
    getGraph: (key) => topo.nodes.includes(key) ? topo : { nodes: [key], edges: [] },
    newId,
  };
}

describe("BUG: off-by-one in LLM round loop", () => {
  it("rounds policy max=2 should allow exactly 2 tool-call rounds", async () => {
    const oracles: Record<string, Oracle> = {
      llm: { policies: [{ type: "timeout", max: 5000 }, { type: "rounds", max: 2 }] },
      tool: { policies: [{ type: "timeout", max: 5000 }] },
    };

    let llmCallCount = 0;

    const nodes = new Map<string, Node>();
    nodes.set("worker", {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: (_layers, ctx) => new Observable<NodeResult>((sub) => {
        ctx.oracle.llm({
          oracle: "llm",
          call: async () => {
            llmCallCount++;
            // Always return tool calls to force continuation
            return { response: "thinking", toolCalls: [{ name: "doWork", args: {} }] };
          },
          executeTool: async () => ({ content: "result of doWork" }),
        }).subscribe({
          next: (r) => { sub.next(r); sub.complete(); },
          error: (e) => sub.error(e),
        });
      }),
    });

    const topo: Graph = { nodes: ["worker"], edges: [] };
    const graph = createEngine({ oracles });

    const signals: Signal[] = [];
    await new Promise<void>((resolve) => {
      graph.exec$(manifest(nodes, topo), "worker", "go").subscribe({
        next: (s) => signals.push(s),
        error: () => resolve(),
        complete: () => resolve(),
      });
    });

    // With rounds max=2, the LLM should be called 2 times before rounds limit kicks in.
    // The loop runs round 0, round 1 (2 iterations), then rounds limit is exceeded.
    expect(llmCallCount, "LLM should be called exactly 2 times for rounds max=2").toBe(2);
  }, 10000);
});
