/// <reference lib="deno.worker" />

import { createWorkerIPC } from "../../lib/ipc/worker.ts";

const ipc = createWorkerIPC({
  slow: () =>
    new Promise((resolve) => setTimeout(() => resolve("done"), 500)),
  hang: () => new Promise(() => {}),
});

self.postMessage({ type: "ready" });
