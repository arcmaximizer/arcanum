/// <reference no-default-lib="true" />
/// <reference lib="deno.worker" />

import { createWorkerIPC } from "../ipc/mod.ts";
import type { EventContext } from "../types.ts";

interface EventProposal {
  from: string;
  to: string;
  input: unknown;
  base?: string;
  metadata: unknown;
  eventId: string;
  moduleUrl: string;
}

interface StateTracker {
  reads: Set<string>;
  writes: Map<string, unknown>;
  localState: Map<string, unknown>;
  fetchValue(key: string): Promise<unknown>;
}

function createStateTracker(
  eventId: string,
  ipc: ReturnType<typeof createWorkerIPC>,
): StateTracker {
  const localState = new Map<string, unknown>();

  return {
    reads: new Set(),
    writes: new Map(),
    localState,

    async fetchValue(key: string): Promise<unknown> {
      if (localState.has(key)) return localState.get(key);
      const raw = await ipc.call<string | null>("getState", {
        key,
        eventId,
      });
      const value = raw === null ? undefined : JSON.parse(raw);
      localState.set(key, value);
      return value;
    },
  };
}

const aborting = new Set<string>();

const ipc = createWorkerIPC();

function checkAbort(eventId: string): void {
  if (aborting.has(eventId)) {
    throw new Error("Event aborted");
  }
}

function createContext(
  eventId: string,
  base: string,
  from: string,
  tracker: StateTracker,
): EventContext {
  return {
    async get(key: string): Promise<unknown> {
      checkAbort(eventId);
      tracker.reads.add(key);
      return tracker.fetchValue(key);
    },

    async set(key: string, value: unknown): Promise<void> {
      checkAbort(eventId);
      tracker.writes.set(key, value);
      tracker.localState.set(key, value);
    },

    async exists(key: string): Promise<boolean> {
      checkAbort(eventId);
      tracker.reads.add(key);
      const value = await tracker.fetchValue(key);
      return value !== undefined;
    },

    async call(app: string, input: unknown): Promise<unknown> {
      checkAbort(eventId);
      const result = await ipc.call("call", {
        from,
        to: app,
        input,
        base,
        metadata: { rootId: eventId },
      });
      return (result as { output: unknown }).output;
    },

    notify(app: string, input: unknown): void {
      checkAbort(eventId);
      ipc.call("notify", {
        from,
        to: app,
        input,
        base,
        metadata: {},
      });
    },
  };
}

// Handle execute requests from runner
ipc.on("execute", async (body) => {
  const proposal = body as EventProposal;
  const { eventId, moduleUrl, input, from, base } = proposal;

  const tracker = createStateTracker(eventId, ipc);

  try {
    const mod = await import(moduleUrl);
    const userspaceFn = mod.default;
    if (typeof userspaceFn !== "function") {
      throw new Error(`Module ${moduleUrl} has no default export function`);
    }

    const ctx = createContext(eventId, base ?? eventId, from, tracker);
    const output = await userspaceFn(from, input, ctx);

    const writes: Record<string, string | null> = {};
    for (const [key, value] of tracker.writes) {
      writes[key] = value === undefined ? null : JSON.stringify(value);
    }

    await ipc.call("result", {
      eventId,
      output,
      reads: [...tracker.reads],
      writes,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    await ipc.call("error", { eventId, error: message }).catch(() => {});
    throw new Error(message);
  }
});

// Handle abort signals from runner
ipc.on("abort", (body) => {
  const { eventId } = body as { eventId: string };
  aborting.add(eventId);
});
