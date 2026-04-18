/**
 * llmrx — Reactive execution engine for autonomous AI systems.
 * Policy-driven oracles. One function, one stream.
 *
 * @packageDocumentation
 */

export {
  // Factory
  createEngine,

  // Core primitives
  Interrupt,
  Denied,
  Aborted,
  PolicyExceeded,

  // Constraint factories (shipped as CustomPolicy)
  constraints,

  // Types — definition
  type AncestryEntry,
  type Oracle,
  type OraclePolicy,
  type Node,
  type Graph,
  type Manifest,
  type MapConfig,
  type EngineDef,
  type Layer,
  type ToolCall,
  type ToolResult,
  type Turn,
  type Policy,
  type CustomPolicy,
  type TimeWindow,

  // Types — runtime
  type Engine,
  type NodeExec,
  type OracleExec,
  type Signal,
  type SignalBase,
  type SignalBody,
  type NodeResult,
  type PolicyAction,
  type PolicyResult,
  type PolicySnapshot,
  type ResolvedInterrupt,
  type InterruptCtx,
  type InterruptResult,
} from "./llmrx.js";
