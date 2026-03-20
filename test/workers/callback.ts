/// <reference lib="deno.worker" />

import { createWorkerIPC } from "../../lib/ipc/worker.ts";

const ipc = createWorkerIPC({
  double: (n) => (n as number) * 2,
  method1: () => "local",
});

self.postMessage({ type: "ready" });
