/**
 * minimal-agent.ts — Smallest possible llmrx agent.
 *
 * One node, one oracle, one policy. Shows the full exec$ → signal flow.
 *
 * Run: npx tsx examples/minimal-agent.ts
 */
import { of } from "rxjs";
import {
  createEngine,
  type Manifest,
  type Node,
  type Graph,
  type Layer,
  type NodeResult,
  type Signal,
} from "llmrx";

// ── 1. Define a node ──

const greeter: Node = {
  loadPrompt(input) {
    return of([
      { name: "system", content: "You are a helpful assistant." },
      { name: "input", content: input },
    ]);
  },
  execute(layers, ctx) {
    // Use the LLM oracle — call your provider, loop tools if any
    return ctx.oracle.llm({
      oracle: "llm",
      async call(layers: Layer[]) {
        // Replace with your real provider (Anthropic, OpenAI, etc.)
        const userInput = layers.find(l => l.name === "input")?.content;
        return {
          response: `Hello! You said: "${userInput}"`,
          // toolCalls: [] — return tool calls here to trigger the tool loop
        };
      },
      // executeTool: (tc, ctx) => { ... } — add tools here
    });
  },
};

// ── 2. Build a manifest ──

const manifest: Manifest = {
  getNode: () => greeter,
  getNodeKeys: () => ["greeter"],
  isNode: (key) => key === "greeter",
  getGraph: (): Graph => ({
    nodes: ["greeter"],
    edges: [],
  }),
};

// ── 3. Create the graph engine ──

const engine = createEngine({
  oracles: {
    llm: {
      policies: [
        { type: "calls", max: 100, window: "day" },
        { type: "timeout", max: 30_000 },
      ],
    },
  },
});

// ── 4. Execute and observe ──

const signals: Signal[] = [];

engine.exec$(manifest, "greeter", "What is llmrx?").subscribe({
  next(signal) {
    signals.push(signal);

    if (signal.type === "node:after") {
      console.log("✅ Output:", signal.output);
    }
  },
  complete() {
    console.log(`\n📊 ${signals.length} signals emitted:`);
    for (const s of signals) {
      console.log(`  ${s.type}`);
    }
  },
  error(err) {
    console.error("❌", err);
  },
});
