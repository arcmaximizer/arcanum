/// <reference lib="deno.worker" />

import { createWorkerIPC } from "../../lib/ipc/worker.ts";

const ipc = createWorkerIPC({
  delay: (ms) =>
    new Promise((resolve) => setTimeout(() => resolve(ms), ms as number)),
});

self.postMessage({ type: "ready" });
