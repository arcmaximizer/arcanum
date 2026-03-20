/// <reference lib="deno.worker" />

import { createWorkerIPC } from "../../lib/ipc/worker.ts";

const ipc = createWorkerIPC({
  compute: (n) => (n as number) * 2,
});

ipc.on("computeAsync", async (n) => {
  const syncResult = await ipc.call<number>("compute", n as number);
  return syncResult + 1;
});

self.postMessage({ type: "ready" });
