/**
 * BUG: When an oracle has two limits of the same type but different windows
 * (e.g. cost:hour and cost:day), mutation targets the wrong counter.
 *
 * Root cause:
 * 1. Interrupt payload doesn't include `window`
 * 2. Mutation resolution does `find()` for first matching limitType, picks wrong window
 * 3. `mutateMax` mutates the wrong counter (hour instead of day)
 * 4. Retry re-triggers the same interrupt → infinite loop (no global retry cap)
 */
import { describe, it, expect } from "vitest";
import { lastValueFrom, toArray, Observable, of } from "rxjs";
import {
  createEngine,
  type Manifest, type Node, type NodeExec, type NodeResult,
  type Layer, type Oracle, type Graph, type Signal,
} from "../src/llmrx.js";

let idCounter = 0;
const newId = (p: string) => `${p}_${++idCounter}`;

function nodeWithExec(fn: (ctx: NodeExec) => Observable<NodeResult>): Node {
  return {
    loadPrompt: (input) => of([{ name: "user", content: input }]),
    execute: (_layers, ctx) => fn(ctx),
  };
}

function manifest(nodeMap: Map<string, Node>, topo: Graph): Manifest {
  return {
    getNode: (k) => { const n = nodeMap.get(k); if (!n) throw new Error(`no node: ${k}`); return n; },
    getNodeKeys: () => [...nodeMap.keys()],
    isNode: (k) => nodeMap.has(k),
    getGraph: (key) => topo.nodes.includes(key) ? topo : { nodes: [key], edges: [] },
    newId,
  };
}

describe("BUG: mutation targets wrong window when oracle has same-type limits with different windows", () => {
  it("mutates the day counter (not hour) when day limit is exceeded", async () => {
    const oracles: Record<string, Oracle> = {
      llm: {
        policies: [
          { type: "cost", max: 0.10, window: "hour", extract: (r: unknown) => ({ cost: (r as { cost: number }).cost }) },
          { type: "cost", max: 0.05, window: "day", extract: (r: unknown) => ({ cost: (r as { cost: number }).cost }), mutable: { "human:approver": true } },
        ],
      },
    };

    let interruptCount = 0;
    const nodes = new Map<string, Node>();
    nodes.set("worker", nodeWithExec((ctx) => {
      return new Observable<NodeResult>((sub) => {
        (async () => {
          // Call 1: cost 0.06. Pre-exec: accumulated=0 < day max 0.05? No, 0 < 0.05 passes.
          // Post-exec: day accumulated = 0.06, hour accumulated = 0.06
          await lastValueFrom(ctx.exec$("llm", async () => ({ cost: 0.06, text: "r1" })));
          // Call 2: pre-exec sees day accumulated=0.06 >= max=0.05 → Interrupt (day is mutable)
          // After mutation, day max should become 1.00, retry should succeed
          const value = await lastValueFrom(ctx.exec$("llm", async () => ({ cost: 0.01, text: "r2" })));
          sub.next({ output: value.text });
          sub.complete();
        })().catch((e) => sub.error(e));
      });
    }));

    const topo: Graph = { nodes: ["worker"], edges: [] };

    const graph = createEngine({
      oracles,
      handleInterrupt: async () => {
        interruptCount++;
        return { decision: "mutate", oracle: "human:approver", limitType: "cost", max: 1.00 };
      },
    });

    const signals: Signal[] = [];
    const result = await Promise.race([
      new Promise<"timeout">((resolve) => setTimeout(() => resolve("timeout"), 3000)),
      new Promise<"done">((resolve) => {
        graph.exec$(manifest(nodes, topo), "worker", "go").subscribe({
          next: (s) => signals.push(s),
          error: () => resolve("done"),
          complete: () => resolve("done"),
        });
      }),
    ]);

    // The bug: mutation targets hour counter, day counter unchanged → infinite interrupt loop
    expect(result, "should complete, not infinite-loop").toBe("done");
    expect(interruptCount, "should only need 1 interrupt").toBe(1);

    // After mutation, the day counter's max should be 1.00
    const snap = graph.snapshot();
    const dayEntry = snap.find((e) => e.key.includes("cost") && e.key.includes("day"));
    const hourEntry = snap.find((e) => e.key.includes("cost") && e.key.includes("hour"));
    expect(dayEntry?.max, "day cost max should be mutated to 1.00").toBe(1.00);
    expect(hourEntry?.max, "hour cost max should remain 0.10").toBe(0.10);
  }, 10000);
});
