/**
 * policy-budget-interrupt.ts — Budget gates with mutable limits.
 *
 * Demonstrates:
 *   - Cost tracking with extract()
 *   - Mutable policies with ceiling functions
 *   - Interrupt → human approval → mutate → retry
 *   - CET trading hours custom policy
 *
 * Run: npx tsx examples/policy-budget-interrupt.ts
 */
import { of, lastValueFrom, toArray } from "rxjs";
import {
  createEngine, Interrupt, constraints,
  type Manifest, type Node, type Graph, type Layer,
  type InterruptCtx, type InterruptResult, type Signal,
} from "llmrx";

// ── Simulated LLM cost ──

interface LLMResponse {
  text: string;
  cost: number;
}

// ── Node: analyst that burns through budget ──

const analyst: Node = {
  loadPrompt(input) {
    return of([
      { name: "system", content: "You are a trading analyst. Analyze the given symbol." },
      { name: "input", content: input },
    ]);
  },
  execute(layers, ctx) {
    return ctx.oracle.llm({
      oracle: "llm",
      async call(layers: Layer[]) {
        // Simulate an expensive LLM call
        const response: LLMResponse = { text: "AAPL looks bullish. Recommend buy.", cost: 6.0 };
        return {
          response: response.text,
          // Returning the full object so extract() can pull cost
          ...response,
        };
      },
    });
  },
};

// ── Manifest ──

const manifest: Manifest = {
  getNode: () => analyst,
  getNodeKeys: () => ["analyst"],
  isNode: (key) => key === "analyst",
  getGraph: (): Graph => ({
    nodes: ["analyst"],
    edges: [],
  }),
};

// ── Graph with budget gate ──

const engine = createEngine({
  oracles: {
    llm: {
      policies: [
        {
          type: "cost",
          max: 10,
          window: "day",
          extract: (r) => ({ cost: (r as LLMResponse).cost ?? 0 }),
          // Who can raise the limit, and by how much
          mutable: {
            "human:manager": (next, _current, _max) => next <= 50,  // ceiling: $50/day
            "human:cfo": true,                                       // no ceiling
          },
        },
        { type: "timeout", max: 30_000 },
        { type: "retries", max: 2, backoff: "exponential" },
      ],
    },
    // Trading hours gate
    "api:market": {
      policies: [
        { type: "calls", max: 200, window: "minute" },
        {
          policy: (action) => {
            const h = new Date(action.timestamp)
              .toLocaleString("en", { timeZone: "CET", hour: "numeric", hour12: false });
            return +h >= 9 && +h < 17
              ? { approval: "allow" as const }
              : { approval: "deny" as const, reason: "outside CET trading hours" };
          },
        },
      ],
    },
  },
  // Global constraints
  policies: [constraints.maxCycles(0), constraints.maxDepth(5)],

  // When budget is exceeded, the engine auto-interrupts. This handler resolves it.
  handleInterrupt: async (interrupt: Interrupt, _ctx: InterruptCtx): Promise<InterruptResult> => {
    console.log("⏸️  Interrupt received:", interrupt.payload);

    // Simulate manager approval — raise budget to $30
    console.log("✅ Manager approves: raising daily LLM cost limit to $30");
    return { decision: "mutate", oracle: "llm", limitType: "cost", max: 30 };
  },
});

// ── Run ──

async function main() {
  const signals = await lastValueFrom(
    engine.exec$(manifest, "analyst", "AAPL").pipe(toArray()),
  );

  console.log(`\n📊 ${signals.length} signals:`);
  for (const s of signals) {
    if (s.type === "node:after") console.log("  ✅ Output:", s.output);
    else if (s.type === "oracle:exceeded") console.log(`  🚨 Exceeded: ${s.limitType} (${s.accumulated}/${s.max})`);
    else if (s.type.startsWith("oracle:mutation")) console.log(`  🔓 Mutation: ${s.type}`);
    else if (s.type === "oracle:denied") console.log(`  🚫 Denied: ${s.reason}`);
    else console.log(`  ${s.type}`);
  }

  // Snapshot for persistence
  const snap = engine.snapshot();
  console.log(`\n💾 Snapshot: ${snap.length} limit entries`);
}

main().catch(console.error);
