export { createHostIPC } from "./host.ts";
export type { HostIPC } from "./api.ts";

export { createWorkerIPC } from "./worker.ts";
export type { WorkerIPC } from "./api.ts";

export type {
  IPCBody,
  IPCRequest,
  IPCResponse,
  IPCMethodHandler,
  IPCOptions,
  IIpc,
} from "./types.ts";
