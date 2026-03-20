/// <reference lib="deno.worker" />

import { createWorkerIPC } from "../../lib/ipc/worker.ts";

const ipc = createWorkerIPC({
  callHost: async (msg) => {
    const response = await ipc.call<string>("processInHost", msg as string);
    return response + " - processed in worker";
  },
});

self.postMessage({ type: "ready" });
