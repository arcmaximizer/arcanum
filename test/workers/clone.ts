/// <reference lib="deno.worker" />

import { createWorkerIPC } from "../../lib/ipc/worker.ts";

const ipc = createWorkerIPC({
  echo: (obj: unknown) => obj,
});

self.postMessage({ type: "ready" });
