/// <reference lib="deno.worker" />

import { createWorkerIPC } from "../../lib/ipc/worker.ts";

const ipc = createWorkerIPC({
  greet: (name) => `Hello, ${name}!`,
  add: (nums) => {
    const arr = nums as number[];
    return arr[0]! + arr[1]!;
  },
  fail: () => {
    throw new Error("Intentional failure");
  },
});

self.postMessage({ type: "ready" });
