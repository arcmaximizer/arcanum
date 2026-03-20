import type { IPCMethodHandler, IPCOptions } from "./types.ts";

export type {
  IIpc,
  IPCBody,
  IPCMethodHandler,
  IPCOptions,
  IPCRequest,
  IPCResponse,
} from "./types.ts";

export interface HostIPC extends IPCOptions {
  call<R = unknown>(method: string, body?: unknown): Promise<R>;
  on(method: string, handler: IPCMethodHandler): void;
  off(method: string): void;
  terminate(): void;
  worker: Worker;
}

export interface WorkerIPC extends IPCOptions {
  call<R = unknown>(method: string, body?: unknown): Promise<R>;
  on(method: string, handler: IPCMethodHandler): void;
  off(method: string): void;
  terminate(): void;
}
