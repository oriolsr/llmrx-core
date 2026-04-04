/**
 * Backoff support for retries policy.
 * { type: "retries", max: 3, backoff: "exponential" | number | (attempt) => number }
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

describe("backoff on retries", () => {
  it("fixed backoff: retries are delayed by fixed ms", async () => {
    const oracles: Record<string, Oracle> = {
      llm: {
        policies: [
          { type: "retries", max: 2, backoff: 100 },
          { type: "timeout", max: 5000 },
        ],
      },
    };

    let callCount = 0;
    const timestamps: number[] = [];

    const nodes = new Map<string, Node>();
    nodes.set("worker", {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: (_layers: Layer[], ctx: NodeExec) => new Observable<NodeResult>((sub) => {
        (async () => {
          const value = await lastValueFrom(
            ctx.exec$("llm", async () => {
              callCount++;
              timestamps.push(Date.now());
              if (callCount <= 2) throw new Error("transient");
              return { text: "ok" };
            }),
          );
          sub.next({ output: value.text });
          sub.complete();
        })().catch((e) => sub.error(e));
      }),
    });

    const topo: Graph = { nodes: ["worker"], edges: [] };
    const graph = createEngine({ oracles });

    await lastValueFrom(
      graph.exec$(manifest(nodes, topo), "worker", "go").pipe(toArray()),
    );

    expect(callCount).toBe(3); // 1 initial + 2 retries
    // Each retry should be delayed by ~100ms
    const gap1 = timestamps[1]! - timestamps[0]!;
    const gap2 = timestamps[2]! - timestamps[1]!;
    expect(gap1, "first retry delay should be ~100ms").toBeGreaterThanOrEqual(80);
    expect(gap2, "second retry delay should be ~100ms").toBeGreaterThanOrEqual(80);
  }, 10000);

  it("exponential backoff: delays double each retry", async () => {
    const oracles: Record<string, Oracle> = {
      llm: {
        policies: [
          { type: "retries", max: 2, backoff: "exponential" },
          { type: "timeout", max: 10000 },
        ],
      },
    };

    let callCount = 0;
    const timestamps: number[] = [];

    const nodes = new Map<string, Node>();
    nodes.set("worker", {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: (_layers: Layer[], ctx: NodeExec) => new Observable<NodeResult>((sub) => {
        (async () => {
          const value = await lastValueFrom(
            ctx.exec$("llm", async () => {
              callCount++;
              timestamps.push(Date.now());
              if (callCount <= 2) throw new Error("transient");
              return { text: "ok" };
            }),
          );
          sub.next({ output: value.text });
          sub.complete();
        })().catch((e) => sub.error(e));
      }),
    });

    const topo: Graph = { nodes: ["worker"], edges: [] };
    const graph = createEngine({ oracles });

    await lastValueFrom(
      graph.exec$(manifest(nodes, topo), "worker", "go").pipe(toArray()),
    );

    expect(callCount).toBe(3);
    const gap1 = timestamps[1]! - timestamps[0]!;
    const gap2 = timestamps[2]! - timestamps[1]!;
    // exponential: attempt 1 → 1000ms, attempt 2 → 2000ms
    expect(gap1, "first retry ~1000ms").toBeGreaterThanOrEqual(800);
    expect(gap2, "second retry ~2000ms").toBeGreaterThanOrEqual(1600);
    expect(gap2, "second retry should be longer than first").toBeGreaterThan(gap1);
  }, 15000);

  it("custom backoff function: receives attempt number", async () => {
    const receivedAttempts: number[] = [];
    const oracles: Record<string, Oracle> = {
      llm: {
        policies: [
          {
            type: "retries", max: 2,
            backoff: (attempt: number) => {
              receivedAttempts.push(attempt);
              return 50 * (attempt + 1); // 50ms, 100ms
            },
          },
          { type: "timeout", max: 5000 },
        ],
      },
    };

    let callCount = 0;
    const timestamps: number[] = [];

    const nodes = new Map<string, Node>();
    nodes.set("worker", {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: (_layers: Layer[], ctx: NodeExec) => new Observable<NodeResult>((sub) => {
        (async () => {
          const value = await lastValueFrom(
            ctx.exec$("llm", async () => {
              callCount++;
              timestamps.push(Date.now());
              if (callCount <= 2) throw new Error("transient");
              return { text: "ok" };
            }),
          );
          sub.next({ output: value.text });
          sub.complete();
        })().catch((e) => sub.error(e));
      }),
    });

    const topo: Graph = { nodes: ["worker"], edges: [] };
    const graph = createEngine({ oracles });

    await lastValueFrom(
      graph.exec$(manifest(nodes, topo), "worker", "go").pipe(toArray()),
    );

    expect(callCount).toBe(3);
    expect(receivedAttempts).toEqual([0, 1]); // 0-indexed attempt numbers
    const gap1 = timestamps[1]! - timestamps[0]!;
    const gap2 = timestamps[2]! - timestamps[1]!;
    expect(gap1, "first retry ~50ms").toBeGreaterThanOrEqual(30);
    expect(gap2, "second retry ~100ms").toBeGreaterThanOrEqual(70);
  }, 10000);

  it("no backoff (default): retries are instant", async () => {
    const oracles: Record<string, Oracle> = {
      llm: {
        policies: [
          { type: "retries", max: 2 },
          { type: "timeout", max: 5000 },
        ],
      },
    };

    let callCount = 0;
    const timestamps: number[] = [];

    const nodes = new Map<string, Node>();
    nodes.set("worker", {
      loadPrompt: (input) => of([{ name: "user", content: input }]),
      execute: (_layers: Layer[], ctx: NodeExec) => new Observable<NodeResult>((sub) => {
        (async () => {
          const value = await lastValueFrom(
            ctx.exec$("llm", async () => {
              callCount++;
              timestamps.push(Date.now());
              if (callCount <= 2) throw new Error("transient");
              return { text: "ok" };
            }),
          );
          sub.next({ output: value.text });
          sub.complete();
        })().catch((e) => sub.error(e));
      }),
    });

    const topo: Graph = { nodes: ["worker"], edges: [] };
    const graph = createEngine({ oracles });

    await lastValueFrom(
      graph.exec$(manifest(nodes, topo), "worker", "go").pipe(toArray()),
    );

    expect(callCount).toBe(3);
    // Without backoff, retries should be near-instant
    const totalTime = timestamps[2]! - timestamps[0]!;
    expect(totalTime, "total time without backoff should be < 50ms").toBeLessThan(50);
  });
});
