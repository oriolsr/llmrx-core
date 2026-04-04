/**
 * Structured edge data flow + ctx.node() layer access.
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

function manifest(nodeMap: Map<string, Node>, topo: Graph): Manifest {
  return {
    getNode: (k) => { const n = nodeMap.get(k); if (!n) throw new Error(`no: ${k}`); return n; },
    getNodeKeys: () => [...nodeMap.keys()],
    isNode: (k) => nodeMap.has(k),
    getGraph: (key) => topo.nodes.includes(key) ? topo : { nodes: [key], edges: [] },
    newId,
  };
}

describe("structured edge data flow", () => {
  it("node outputs object → downstream receives it in input array", async () => {
    let receivedInput: unknown = null;

    const nodes = new Map<string, Node>();
    nodes.set("api", {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: () => of({ output: { AAPL: 185.20, TSLA: 242.50 } }),
    });
    nodes.set("consumer", {
      loadPrompt: (input, ctx) => {
        receivedInput = input;
        return of([{ name: "user", content: input }]);
      },
      execute: () => of({ output: "done" }),
    });

    const topo: Graph = { nodes: ["api", "consumer"], edges: [{ from: "api", to: "consumer" }] };
    const graph = createEngine({ oracles: {} });

    await lastValueFrom(graph.exec$(manifest(nodes, topo), "api", "fetch prices").pipe(toArray()));

    // consumer should receive an array of predecessor outputs
    expect(Array.isArray(receivedInput), "input should be an array").toBe(true);
    const inputs = receivedInput as unknown[];
    expect(inputs.length).toBe(1);
    expect(inputs[0]).toEqual({ AAPL: 185.20, TSLA: 242.50 });
  });

  it("diamond DAG — node D receives outputs from both B and C", async () => {
    let receivedInput: unknown = null;

    const nodes = new Map<string, Node>();
    nodes.set("A", {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: () => of({ output: "from-A" }),
    });
    nodes.set("B", {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: () => of({ output: { source: "B", data: 42 } }),
    });
    nodes.set("C", {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: () => of({ output: ["C-item-1", "C-item-2"] }),
    });
    nodes.set("D", {
      loadPrompt: (input) => {
        receivedInput = input;
        return of([{ name: "user", content: input }]);
      },
      execute: () => of({ output: "done" }),
    });

    const topo: Graph = {
      nodes: ["A", "B", "C", "D"],
      edges: [
        { from: "A", to: "B" },
        { from: "A", to: "C" },
        { from: "B", to: "D" },
        { from: "C", to: "D" },
      ],
    };

    const graph = createEngine({ oracles: {} });
    await lastValueFrom(graph.exec$(manifest(nodes, topo), "A", "go").pipe(toArray()));

    // D receives outputs from B and C as an array
    expect(Array.isArray(receivedInput)).toBe(true);
    const inputs = receivedInput as unknown[];
    expect(inputs.length).toBe(2);
    // B and C run in parallel so order matches edge order (B first, C second)
    expect(inputs).toContainEqual({ source: "B", data: 42 });
    expect(inputs).toContainEqual(["C-item-1", "C-item-2"]);
  });

  it("root node receives raw message wrapped in array", async () => {
    let receivedInput: unknown = null;

    const nodes = new Map<string, Node>();
    nodes.set("root", {
      loadPrompt: (input) => {
        receivedInput = input;
        return of([{ name: "user", content: input }]);
      },
      execute: () => of({ output: "done" }),
    });

    const topo: Graph = { nodes: ["root"], edges: [] };
    const graph = createEngine({ oracles: {} });

    await lastValueFrom(graph.exec$(manifest(nodes, topo), "root", "hello world").pipe(toArray()));

    // Root receives [message] — single-element array with the raw input
    expect(Array.isArray(receivedInput)).toBe(true);
    const inputs = receivedInput as unknown[];
    expect(inputs.length).toBe(1);
    expect(inputs[0]).toBe("hello world");
  });
});

describe("ctx.node() — ancestor layer access", () => {
  it("downstream node can access ancestor layers and output", async () => {
    let capturedNode: { layers: Layer[]; output: unknown } | null = null;

    const nodes = new Map<string, Node>();
    nodes.set("analyst", {
      loadPrompt: (input) => of([
        { name: "soul", content: "You are an analyst" },
        { name: "memory", content: "Previous: AAPL +12%" },
        { name: "input", content: input },
      ]),
      execute: () => of({ output: { recommendation: "buy AAPL" } }),
    });
    nodes.set("writer", {
      loadPrompt: (input, ctx) => {
        capturedNode = ctx.node("analyst");
        return of([{ name: "user", content: input }]);
      },
      execute: () => of({ output: "report done" }),
    });

    const topo: Graph = { nodes: ["analyst", "writer"], edges: [{ from: "analyst", to: "writer" }] };
    const graph = createEngine({ oracles: {} });

    await lastValueFrom(graph.exec$(manifest(nodes, topo), "analyst", "analyze").pipe(toArray()));

    expect(capturedNode, "ctx.node('analyst') should return state").not.toBeNull();
    expect(capturedNode!.layers).toHaveLength(3);
    expect(capturedNode!.layers[0]!.name).toBe("soul");
    expect(capturedNode!.layers[1]!.name).toBe("memory");
    expect(capturedNode!.output).toEqual({ recommendation: "buy AAPL" });
  });

  it("ctx.node() returns null for nodes not yet executed", async () => {
    let capturedNode: { layers: Layer[]; output: unknown } | null | undefined;

    const nodes = new Map<string, Node>();
    nodes.set("first", {
      loadPrompt: (input, ctx) => {
        capturedNode = ctx.node("second"); // second hasn't run yet
        return of([{ name: "user", content: input }]);
      },
      execute: () => of({ output: "done" }),
    });
    nodes.set("second", {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: () => of({ output: "done" }),
    });

    const topo: Graph = { nodes: ["first", "second"], edges: [{ from: "first", to: "second" }] };
    const graph = createEngine({ oracles: {} });

    await lastValueFrom(graph.exec$(manifest(nodes, topo), "first", "go").pipe(toArray()));

    expect(capturedNode).toBeNull();
  });
});
